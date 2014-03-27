# (c) 2014 -- onwards Georgios Gousios <gousiosg@gmail.com>
#
# Distributed under the 2-close BSD license, see top level directory.

require 'sequel'
require 'ghtorrent'
require 'formats'

module DBStuff

  include GHTorrent::Settings
  include GHTorrent::Utils
  include GHTorrent::Logging

  Sequel.extension :migration

  def db(check_if_connected = false)

    unless @db.nil?
      if check_if_connected
        begin
          @db["select now()"].first
          return @db
        rescue
          logger.warn "DB: connection to #{config(:sql_url)} is down"
        end
      else
        return @db
      end
    end

    Sequel.single_threaded = true
    database = Sequel.connect(config(:sql_url), :encoding => 'utf8')
    logger.debug "DB: Connected to #{config(:sql_url)}"

    if database.tables.empty?
      logger.warn 'DB: Database empty, creating schema'

      logger.info 'Creating table users'
      database.create_table :users do
        primary_key :id
        String :name, :null => false
        String :email, :null => false, :unique => true
        DateTime :created_at, :null => false, :default => Sequel::CURRENT_TIMESTAMP
      end

      logger.info 'Creating table repos'
      database.create_table :repos do
        primary_key :id
        String :name, :null => false, :unique => true
        DateTime :created_at, :null => false, :default => Sequel::CURRENT_TIMESTAMP
      end

      logger.info 'Creating table requests'
      database.create_table :requests do
        primary_key :id
        foreign_key :user_id, :users, :null => false
        String :hash, :size => 40, :unique => true
        TrueClass :backup_done, :null => false, :default => false
        DateTime :created_at, :null => false
        DateTime :updated_at, :null => false, :default => Sequel::CURRENT_TIMESTAMP
      end

      logger.info 'Creating table request_contents'
      database.create_table :request_contents do
        primary_key :id
        foreign_key :request_id, :requests, :null => false
        foreign_key :repo_id, :repos, :null => false
        TrueClass :done, :null => false, :default => false
        DateTime :created_at, :null => false
        DateTime :updated_at, :null => false, :default => Sequel::CURRENT_TIMESTAMP
      end

      logger.info 'Creating table request_contents_status'
      database.create_table :request_contents_status do
        primary_key :id
        foreign_key :request_content_id, :request_contents, :null => false
        String :text
        String :status
        DateTime :created_at, :null => false, :default => Sequel::CURRENT_TIMESTAMP
      end
    end

    @db = database
    @db
  end

  def db_close
    logger.debug "Closing connection to #{config(:sql_url)}"
    @db.disconnect unless @db.nil?
    @db = NIL
  end

end
