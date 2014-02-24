#!/usr/bin/env ruby
#
# (c) 2014 -- onwards Georgios Gousios <gousiosg@gmail.com>
#
# Distributed under the 2-close BSD license, see top level directory.

require 'db_stuff'
require 'queue_stuff'
require 'ghtorrent'
require 'json'

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
          details = msg.split(/ /)[4..-1].join(' ').tr('@',' ').strip

          if job_id.nil? or not is_number?(job_id) or owner.nil? \
            or repo.nil? or status.nil? \
            or not %w(FAILED WORKING FINISHED STOPPED).include?(status)

            warn "StatusUpdater: Bad message: [#{msg}] ignoring"
            amqp_channel.acknowledge(delivery_info.delivery_tag, false)
            next
          end

          db.transaction(:rollback => :reraise, :isolation => :committed) do
            req_contents = db.from(:users, :repos, :requests, :request_contents)\
                             .where(:users__id => :requests__user_id)\
                             .where(:request_contents__repo_id => :repos__id)\
                             .where(:requests__id => :request_contents__request_id)\
                             .where(:requests__id => job_id.to_i)\
                             .where(:repos__name => "#{owner}/#{repo}")\
                             .select(:request_contents__id, :users__email, :repos__name)
                             .first

            # Ignore updates to inexistent jobs
            if req_contents.nil?
              warn "StatusUpdater: Msg: [#{msg}] pointing to inexisting job. Ignoring"
              amqp_channel.acknowledge(delivery_info.delivery_tag, false)
              next
            end

            # Write the new job status for the job/project
            db[:request_contents_status].insert(
                :request_content_id => req_contents[:id],
                :status => status,
                :text => details
            )
            debug "StatusUpdater: Set status for #{job_id} (by #{req_contents[:email]}) repo: #{req_contents[:name]} -> #{status} (#{details})"

            # If a terminating msg arrives, update the request status to done
            if %w(FAILED FINISHED STOPPED).include?(status)
              rc = db.from(:request_contents, :repos)\
                     .where(:request_contents__repo_id => :repos__id)\
                     .where(:repos__name => "#{owner}/#{repo}")\
                     .where(:request_contents__request_id => job_id.to_i)
                     .select(:request_contents__id)
                     .first

              db[:request_contents].where(:id => rc[:id]).update(
                  :done => true,
                  :updated_at => Time.now)
              debug "StatusUpdater: Set finished flag for job id: #{job_id} (by #{req_contents[:email]}) repo: #{req_contents[:name]}"
            end

            # If all projects are done for the request, start a backup
            rcs = db.from(:request_contents)\
                    .where(:request_contents__request_id => job_id.to_i)\
                    .where(:request_contents__done => false)\
                    .all

            if rcs.nil? or rcs.size == 0
              debug "StatusUpdater: Starting backup for job #{req_contents[:email]} -> #{job_id}"
              repos = db.from(:request_contents, :repos)\
                        .where(:repos__id => :request_contents__repo_id)\
                        .where(:request_contents__request_id => msg.to_i)\
                        .select(:repos__name)\
                        .all

              u_details = db.from(:users, :requests)\
                            .where(:users__id => :requests__user_id)\
                            .where(:requests__id => msg.to_i)\
                            .select(:users__name, :users__email, :requests__hash)\
                            .first

              backup_job         = {}
              backup_job[:id]    = job_id
              backup_job[:repos] = repos
              backup_job[:uname] = u_details[:name]
              backup_job[:email] = u_details[:email]
              backup_job[:hash]  = u_details[:hash]
              amqp_exchange.publish(backup_job.to_json,
                                    {:timestamp => Time.now.to_i,
                                     :persistent => true,
                                     :routing_key => BACKUP_QUEUE_ROUTEKEY})
            end

            amqp_channel.acknowledge(delivery_info.delivery_tag, false)
          end
        end
      rescue Bunny::TCPConnectionFailed => e
        warn "StatusUpdater: Connection to #{config(:amqp_host)} failed. Retrying in 1 sec"
        sleep(1)
      rescue Bunny::NotFound, Bunny::AccessRefused, Bunny::PreconditionFailed => e
        warn "StatusUpdater: Channel error: #{e}. Retrying in 1 sec"
        sleep(1)
      rescue Interrupt
        stopped = true
      rescue Exception => e
        logger.error e.message
        logger.error e.backtrace.join("\n")
      ensure
        db_close
      end
    end
  end

  def is_number?(obj)
    obj.to_s == obj.to_i.to_s
  end
end

StatusUpdater.run
