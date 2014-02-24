# (c) 2014 -- onwards Georgios Gousios <gousiosg@gmail.com>
#
# Distributed under the 2-close BSD license, see top level directory.

require 'ghtorrent'

module QueueStuff

  include GHTorrent::Settings

  JOB_QUEUE = 'ghtorrent-service-job'
  JOB_QUEUE_ROUTEKEY = 'ghtorrent.service.job'

  RESULT_QUEUE = 'ghtorrent-service-result'
  RESULT_QUEUE_ROUTEKEY = 'ghtorrent.service.result'

  BACKUP_QUEUE = 'ghtorrent-service-backup'
  BACKUP_QUEUE_ROUTEKEY = 'ghtorrent.service.backup'

  def amqp_connection
    if @amqp_con.nil? or @amqp_con.closed?
      @amqp_con = Bunny.new(:host => config(:amqp_host),
                            :port => config(:amqp_port),
                            :username => config(:amqp_username),
                            :password => config(:amqp_password))
      @amqp_con.start
      debug "Connection to #{config(:amqp_host)} succeeded"
      @amqp_channel = nil
    end
    @amqp_con
  end

  def amqp_channel
    if @amqp_channel.nil? or @amqp_channel.closed?
      @amqp_channel ||= amqp_connection.create_channel
      debug "Setting prefetch to #{config(:amqp_prefetch)}"
      @amqp_channel.prefetch(config(:amqp_prefetch))
      @amqp_exch = nil
      @amqp_queue = nil
    end
    @amqp_channel
  end

  def amqp_exchange
    amqp_channel
    @amqp_exch ||= amqp_channel.topic(config(:amqp_exchange), :durable => true,
                                      :auto_delete => false)
  end

  def amqp_queue(queue_name)
    amqp_channel
    @amqp_queues ||= Hash.new
    @amqp_queues[queue_name] ||= amqp_channel.queue(queue_name, :durable => true)
    @amqp_queues[queue_name]
  end

  def consumer_queue(queue_name, routing_key)
    queue = amqp_queue(queue_name)
    queue.bind(amqp_exchange, :routing_key => routing_key)
    queue
  end

  def amqp_close
    amqp_channel.close unless @amqp_channel.nil?
    amqp_connection.close unless @amqp_connection.nil?
  end

end
