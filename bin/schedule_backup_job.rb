#!/usr/bin/env ruby

require 'db_stuff'
require 'queue_stuff'
require 'ghtorrent'
require 'json'
require 'yaml'

include QueueStuff
include DBStuff
include GHTorrent::Logging

job_id = ARGV[0].to_i

def settings
  YAML.load_file(ARGV[1])
end

def logger
  Logger.new STDOUT
end

repos = db.from(:request_contents, :repos)\
  .where(:repos__id => :request_contents__repo_id)\
  .where(:request_contents__request_id => job_id)\
  .select(:repos__name)\
  .all

u_details = db.from(:users, :requests)\
  .where(:users__id => :requests__user_id)\
  .where(:requests__id => job_id)\
  .select(:users__name, :users__email, :requests__hash)\
  .first

backup_job         = {}
backup_job[:id]    = job_id
backup_job[:repos] = repos
backup_job[:uname] = u_details[:name]
backup_job[:email] = u_details[:email]
backup_job[:hash]  = u_details[:hash]

puts backup_job.to_json
amqp_exchange.publish(backup_job.to_json,
                      {:timestamp => Time.now.to_i,
                       :persistent => true,
                       :routing_key => BACKUP_QUEUE_ROUTEKEY})
