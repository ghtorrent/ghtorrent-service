require 'db_stuff'
require 'queue_stuff'
require 'ghtorrent'
require 'mongo'

class Backuper < GHTorrent::Command

  include GHTorrent::Persister
  include GHTorrent::Settings
  include GHTorrent::Logging
  include QueueStuff
  include DBStuff

  def go
    stopped = false

    while not stopped
      begin
        consumer_queue(BACKUP_QUEUE, BACKUP_QUEUE_ROUTEKEY).subscribe(
            :block => true, :ack => true) do |delivery_info, properties, msg|

          job = begin
            JSON.parse(msg)
          rescue
            warn "Backuper: Cannot parse JSON string: #{msg}"
          end

          if job.nil?
            warn "Backuper: Invalid message #{msg}"
            amqp_channel.acknowledge(delivery_info.delivery_tag, false)
            next
          end

          db_name = Formats::DB_NAME % job['id']

          db_url = URI(config(:sql_url))
          db_url.path = "/#{db_name}"
          @settings = merge_config_values(@settings, {:sql_url => db_url.to_s})

          backup_path = File.join('backups', job['id'])
          FileUtils.mkdir_p(backup_path)
          repos = job['repos'].map{|k,v| v}

          #users(backup_path)
          #commits(backup_path)
          #commit_comments(backup_path)
          #repos(backup_path)

          %w(repo_labels repo_collaborators watchers forks pull_requests \
             pull_request_comments issues issue_events issue_comments).each do |collection|
            repo_bound(backup_path, collection)
          end

          db_close
          amqp_channel.acknowledge(delivery_info.delivery_tag, false)
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
      r = mongo['users'].find({'login' => x[:login]}).to_a
      unless r.empty?
        out.write BSON::serialize(r[0])
      end
    end
    out.close
  end

  def commits(dir)
    out = File.open(File.join(dir, 'commits.bson'), 'w+')
    db[:commits].select(:sha).each do |x|
      r = mongo['commits'].find({'sha' => x[:sha]}).to_a
      unless r.empty?
        out.write BSON::serialize(r[0])
      end
    end
    out.close
  end

  def commit_comments(dir)
    out = File.open(File.join(dir, 'commit_comments.bson'), 'w+')
    db[:commit_comments].select(:comment_id).each do |x|
      r = mongo['commit_comments'].find({'id' => x[:comment_id]}).to_a
      unless r.empty?
        out.write BSON::serialize(r[0])
      end
    end
    out.close
  end

  def repos(dir)
    out = File.open(File.join(dir, 'repos.bson'), 'w+')
    db[:projects, :users].where(:projects__owner_id => :users__id)\
                         .select(:users__login, :projects__name)\
                         .each do |x|
      r = mongo['repos'].find({'name' => x[:name], 'owner.login' => x[:login]}).to_a
      unless r.empty?
        out.write BSON::serialize(r[0])
      end
    end
    out.close
  end

  def repo_bound(backup_path, collection)
    mongo_command = "mongodump -h #{@settings['mongo']['host']} --port #{@settings['mongo']['port']} -d #{@settings['mongo']['db']} -c %s -q \"%s\"  -o - >> %s"
    output = File.join(backup_path, "#{collection.to_s}.json" )

    db[:projects, :users].where(:projects__owner_id => :users__id)\
                         .select(:users__login, :projects__name)\
                         .each do |x|
      debug "Dumping #{collection.to_s} for #{x[:login]}/#{x[:name]}"
      query = "{'owner': '#{x[:login]}', 'repo': '#{x[:name]}'}"
      cmd = sprintf(mongo_command, collection.to_s, query, output)
      system(cmd)
    end
  end

end

Backuper.run