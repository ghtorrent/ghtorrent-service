require 'sequel'
require 'ghtorrent'
require 'formats'

module DBStuff

  include GHTorrent::Settings
  include GHTorrent::Utils
  include GHTorrent::Logging

  Sequel.extension :migration

  def db
    return Thread.current[:db] unless Thread.current[:db].nil?

    Sequel.single_threaded = true
    database = Sequel.connect(config(:sql_url), :encoding => 'utf8')

    if database.tables.empty?
      logger.warn 'Database empty, creating schema'

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

    Thread.current[:db] = database
    Thread.current[:db]
  end

  def db_close
    Thread.current[:db].disconnect unless Thread.current[:db].nil?
    Thread.current[:db] == NIL
  end

end
