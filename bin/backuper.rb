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

          FileUtils.mkdir_p(FileUtils.mkpath('backups', job['id']))
          repos = job['repos'].map{|k,v| v}

          #mongo_command = "mongodump -h #{config(:mongo_host)} -d #{config(:mongo_db)} -c %s -q %s -o - >> %s"

          # Users
          users(dir)

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
  end

  def mongo
    @mongo ||= Mongo::Connection.new(config(:mongo_host),
                                     config(:mongo_port)).db(config(:mongo_db))
    @mongo
  end

  def users(dir)
    out = File.open(FileUtils.mkpath(dir, 'users.bson'), 'w+')
    db[:users].select(:login).all.each do |x|
      r = mongo[:users].find({'login' => x})
      out.write r
    end
    out.close
  end

end

Backuper.run