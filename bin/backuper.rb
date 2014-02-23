require 'db_stuff'
require 'queue_stuff'
require 'email_stuff'
require 'ghtorrent'
require 'mongo'

class Backuper < GHTorrent::Command

  include GHTorrent::Persister
  include GHTorrent::Settings
  include GHTorrent::Logging
  include QueueStuff
  include DBStuff
  include EmailStuff

  def go
    stopped = false

    while not stopped
      begin
        consumer_queue(BACKUP_QUEUE, BACKUP_QUEUE_ROUTEKEY).subscribe(
            :block => true, :ack => true) do |delivery_info, properties, msg|
          
          amqp_channel.acknowledge(delivery_info.delivery_tag, false)

          job = begin
            JSON.parse(msg)
          rescue
            warn "Backuper: Cannot parse JSON string: #{msg}"
          end

          if job.nil?
            warn "Backuper: Invalid message #{msg}"
            next
          end

          db_name = Formats::DB_NAME % job['id']

          db_url = URI(config(:sql_url))
          db_url.path = "/#{db_name}"
          @settings = merge_config_values(@settings, {:sql_url => db_url.to_s})

          backup_path = File.join(@settings['dump']['tmp'], job['id'].to_s)
          FileUtils.mkdir_p(backup_path)

          begin
            # individual collections
            %w(users commits commit_comments
               repos followers org_members).each do |method|
              debug "Backuper: Dumping #{method} for #{job['email']} -> #{job['id']}"
              send method, backup_path
            end

            # repo-bound collections
            %w(repo_labels repo_collaborators watchers forks
              pull_requests pull_request_comments issues
               issue_events issue_comments).each do |collection|
              debug "Backuper: Dumping #{collection.to_s} for #{job['email']} ->  #{job['id']}"
              repo_bound(backup_path, collection)
            end

            debug "Backuper: Backing up MySQL for #{job['email']} -> #{job['id']}"
            mysql(backup_path, db_name)

            dumpname = "ght-#{job['hash']}.tar.gz"
            cmd = "cd #{@settings['dump']['tmp']} && tar zcvf #{dumpname} #{job['id']}"
            system(cmd)

            FileUtils.mv(File.join(@settings['dump']['tmp'], dumpname), @settings['dump']['dir'])

            url = @settings['dump']['url_prefix'] + '/' + dumpname
            send_dump_succeed(job['email'], job['uname'], url)
            debug "Backuper: Backing done for #{job['email']} -> #{job['id']}"
          rescue Exception => e
            send_dump_failed(job[:email], job[:uname], e.message)
            send_dump_exception([e.message, e.backtrace.join("\n")].join("\n\n"))
            raise e
          ensure
            db_close
          end
        end
      rescue Bunny::TCPConnectionFailed => e
        warn "Backuper: Connection to #{config(:amqp_host)} failed. Retrying in 1 sec"
        sleep(1)
      rescue Bunny::NotFound, Bunny::AccessRefused, Bunny::PreconditionFailed => e
        warn "Backuper: Channel error: #{e}. Retrying in 1 sec"
        sleep(1)
      rescue Interrupt
        stopped = true
      rescue Exception => e
        logger.error e.message
        logger.error e.backtrace.join("\n")
      end
    end
    amqp_close
  end

  def mongo
    @mongo ||= Mongo::Connection.new(@settings['mongo']['host'],
                                     @settings['mongo']['port'])\
                                .db(@settings['mongo']['db'])
    @mongo
  end

  def users(dir)
    out = File.open(File.join(dir, 'users.bson'), 'w+')
    db[:users].select(:login).each do |x|
      safe_retrieve do
        r = mongo['users'].find({'login' => x[:login]}).to_a
        unless r.empty?
          out.write BSON::serialize(r[0])
        end
      end
    end
    out.close
  end

  def commits(dir)
    out = File.open(File.join(dir, 'commits.bson'), 'w+')
    db[:commits].select(:sha).each do |x|
      safe_retrieve do
        r = mongo['commits'].find({'sha' => x[:sha]}).to_a
        unless r.empty?
          out.write BSON::serialize(r[0])
        end
      end
    end
    out.close
  end

  def commit_comments(dir)
    out = File.open(File.join(dir, 'commit_comments.bson'), 'w+')
    db[:commit_comments].select(:comment_id).each do |x|
      safe_retrieve do
        r = mongo['commit_comments'].find({'id' => x[:comment_id]}).to_a
        unless r.empty?
          out.write BSON::serialize(r[0])
        end
      end
    end
    out.close
  end

  def repos(dir)
    out = File.open(File.join(dir, 'repos.bson'), 'w+')
    db[:projects, :users].where(:projects__owner_id => :users__id)\
                         .select(:users__login, :projects__name)\
                         .each do |x|
      safe_retrieve do
        r = mongo['repos'].find({'name' => x[:name], 'owner.login' => x[:login]}).to_a
        unless r.empty?
          out.write BSON::serialize(r[0])
        end
      end
    end
    out.close
  end

  def followers(dir)
    out = File.open(File.join(dir, 'followers.bson'), 'w+')
    db[:followers, :users].where(:followers__user_id => :users__id)\
                          .select(:users__login)\
                          .each do |x|
      safe_retrieve do
        r = mongo['followers'].find({'login' => x[:login]}).to_a
        unless r.empty?
          out.write BSON::serialize(r[0])
        end
      end
    end
    out.close
  end

  def org_members(dir)
    out = File.open(File.join(dir, 'org_members.bson'), 'w+')

    db[:users].where(:users__type => 'ORG').select(:users__login).each do |x|
      safe_retrieve do
        r = mongo['org_members'].find({'org' => x[:login]}).to_a
        unless r.empty?
          out.write BSON::serialize(r[0])
        end
      end
    end
    out.close
  end


  def repo_bound(backup_path, collection)
    out = File.open(File.join(backup_path, "#{collection.to_s}.bson"), 'w+')

    db[:projects, :users].where(:projects__owner_id => :users__id)\
                         .select(:users__login, :projects__name)\
                         .each do |x| 

      safe_retrieve do
        r = mongo[collection].find({'owner' => "#{x[:login]}", 'repo' => "#{x[:name]}"}).to_a
        unless r.empty?
          r.each do |item|
            out.write BSON::serialize(item)
          end
        end
      end
    end
    out.close
  end

  def mysql(backup_path, db)
    file = File.join(backup_path, 'mysql.sql')
    url = URI(config(:sql_url))
    cmd = "mysqldump -u #{url.user} --password=#{url.password} -h #{url.host} #{db} > #{file}"
    system(cmd)
  end

  def safe_retrieve(&block)
    begin
      yield block
    rescue Exception => e
      logger.error e.message
      logger.error e.backtrace.join("\n")
    end
  end

end

Backuper.run
