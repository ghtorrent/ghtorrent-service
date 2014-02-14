require 'sequel'

module DBStuff

  include GHTorrent::Settings

  Sequel.extension :migration

  def db
    return @db unless @db.nil?

    Sequel.single_threaded = true
    @db = Sequel.connect(config(:sql_url), :encoding => 'utf8')

    if @db.tables.empty?
      logger.warn 'Database empty, creating schema'

      logger.info 'Creating table users'
      @db.create_table :users do
        primary_key :id
        String :name, :null => false
        String :email, :null => false, :unique => true
        DateTime :created_at, :null => false, :default => Sequel::CURRENT_TIMESTAMP
      end

      logger.info 'Creating table repos'
      @db.create_table :repos do
        primary_key :id
        String :name, :null => false, :unique => true
        DateTime :created_at, :null => false, :default => Sequel::CURRENT_TIMESTAMP
      end

      logger.info 'Creating table requests'
      @db.create_table :requests do
        primary_key :id
        foreign_key :user_id, :users, :null => false
        String :hash, :size => 40, :unique => true
        DateTime :created_at, :null => false, :default => Sequel::CURRENT_TIMESTAMP
        DateTime :updated_at, :null => false, :default => Sequel::CURRENT_TIMESTAMP
      end

      logger.info 'Creating table request_contents'
      @db.create_table :request_contents do
        primary_key :id
        foreign_key :request_id, :requests, :null => false
        foreign_key :repo_id, :repos, :null => false
        TrueClass :done, :null => false, :default => false
        DateTime :updated_at, :null => false, :default => Sequel::CURRENT_TIMESTAMP
      end

      logger.info 'Creating table request_contents_status'
      @db.create_table :request_contents_status do
        primary_key :id
        foreign_key :request_content_id, :request_contents, :null => false
        String :text
        String :status
        DateTime :created_at, :null => false, :default => Sequel::CURRENT_TIMESTAMP
      end
    end

    @db
  end

end