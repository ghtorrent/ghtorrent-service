require 'db_stuff'
require 'queue_stuff'
require 'ghtorrent'

class StatusUpdater < GHTorrent::Command

  include GHTorrent::Settings
  include GHTorrent::Logging
  include QueueStuff
  include DBStuff

  def prepare_options(options)
    options.banner <<-BANNER
Connect to a queue and

#{command_name} [options]
    BANNER
  end

  def go

    stopped = false
    while not stopped
      begin
        consumer_queue(RESULT_QUEUE, RESULT_QUEUE_ROUTEKEY).subscribe(
            :block => true, :ack => true) do |delivery_info, properties, msg|

          job_id, owner, repo, status, details = msg.split(/ /)
          details = details.strip[1..-1]

          if job_id.nil? or not is_number?(job_id) or owner.nil? \
            or repo.nil? or status.nil? \
            or not %w(FAILED WORKING FINISHED STOPPED).include?(status)

            warn "Bad message: [#{msg}] ignoring"
            amqp_channel.acknowledge(delivery_info.delivery_tag, false)
          end

          req_contents = db.from(:request_contents, :repos)\
                           .where(:request_contents__repo_id => :repos__id)\
                           .where(:repos__name => "#{owner}/#{repo}")\
                           .where(:request_contents__request_id => job_id.to_i)\
                           .first

          if req_contents.nil?
            warn "Msg: [#{msg}] pointing to inexisting job. Ignoring"
            amqp_channel.acknowledge(delivery_info.delivery_tag, false)
          end

          db[:request_contents_status].insert(
              :request_content_id => req_contents[:id],
              :status => status,
              :msg => details
          )

          if %w(FAILED FINISHED STOPPED).include?(status)
            db.from(:request_contents, :repos)\
               .where(:request_contents__repo_id => :repos__id)\
               .where(:repos__name => "#{owner}/#{repo}")\
               .where(:request_contents__request_id => job_id.to_i)\
               .update(:done => true)
          end

          db.from(:request_contents)\
             .where(:request_contents__request_id => job_id.to_i)\
             .where(:request_contents__done => false)\
             .all


          amqp_channel.acknowledge(delivery_info.delivery_tag, false)
        end
      rescue Bunny::TCPConnectionFailed => e
        warn "Connection to #{config(:amqp_host)} failed. Retrying in 1 sec"
        sleep(1)
      rescue Bunny::NotFound, Bunny::AccessRefused, Bunny::PreconditionFailed => e
        warn "Channel error: #{e}. Retrying in 1 sec"
        sleep(1)
      rescue Interrupt
        stopped = true
      end
    end
  end


  def is_number?(obj)
    obj.to_s == obj.to_i.to_s
  end
end

StatusUpdater.run