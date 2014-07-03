#!/usr/bin/env ruby
#
# (c) 2014 -- onwards Georgios Gousios <gousiosg@gmail.com>
#
# Distributed under the 2-close BSD license, see top level directory.

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

puts "Restarting job #{job_id}"

repos = db.from(:request_contents, :repos)\
  .where(:repos__id => :request_contents__repo_id)\
  .where(:request_contents__request_id => job_id)\
  .select(:repos__name)\
  .all

repos.each do |repo|
  (owner, repository) = repo[:name].split(/\//)
  job = "#{job_id} #{owner} #{repository}" 
  puts "Scheduling #{job}"
  amqp_exchange.publish(job,
                        {:timestamp => Time.now.to_i,
                         :persistent => true,
                         :routing_key => JOB_QUEUE_ROUTEKEY})
end

