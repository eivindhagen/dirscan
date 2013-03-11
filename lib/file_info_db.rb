# FileInfoDb is a class that uses DataMapper to store FileInfo objects in a SQLite3 database

# dependencies
require 'pathname'

require 'rubygems'
require 'data_mapper' # requires all the gems listed above
require 'dm-sqlite-adapter' # SQLite3 support with DataMapper

require File.expand_path('logging', File.dirname(__FILE__))

#
# global setup
#

# If you want the logs displayed you have to do this before the call to setup
DataMapper::Logger.new('log/datamapper.log', :debug)

# An in-memory Sqlite3 connection:
DataMapper.setup(:default, 'sqlite::memory:')
DataMapper.finalize


# wrapper class, which represents the database file itself
class FileInfoDb
  # Mix in the ability to log stuff ...
  include Logging

  class FileInfo
    include DataMapper::Resource

    def self.default_repository_name
      :file_info
    end

    property :id,       Serial    # auto-increment integer key
    property :type,     Integer,  index: :type
    property :name,     String,   index: :name
    property :size,     Integer,  index: :size
    property :mode,     String,   index: :mode
    property :mtime,    Integer,  index: :mtime
    property :own,      String,   index: :own
    property :grp,      String,   index: :grp
    property :sha256,   Text,     index: :sha256, lazy: false
    property :path,     Text,     index: :path, lazy: false
  end

  # init new instance
  public
  def initialize(path)
    logger.debug "initialize(#{path})"
    is_existing_db = File.exist?(path)

    # A Sqlite3 connection to a persistent database
    db_url = "sqlite://#{File.expand_path(path)}"
    @adapter = DataMapper.setup(:file_info, db_url)

    DataMapper.repository(:file_info) do
      DataMapper.finalize

      if is_existing_db
        DataMapper.auto_upgrade!  # adapt existing DB to our needs
      else
        DataMapper.auto_migrate!  # drop & creata tables to get empty tables in pristine condition
      end
    end
  end

end

