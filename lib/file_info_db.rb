require 'rubygems'
require 'data_mapper' # requires all the gems listed above
require 'dm-sqlite-adapter' # SQLite3 support with DataMapper

require 'pathname'

# If you want the logs displayed you have to do this before the call to setup
DataMapper::Logger.new('log/datamapper.log', :debug)

# class DummyModel
#   include DataMapper::Resource

#   property :id,       Serial    # auto-increment integer key
# end

# An in-memory Sqlite3 connection:
DataMapper.setup(:default, 'sqlite::memory:')
DataMapper.finalize


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


class FileInfoDb

  # init new instance
  public
  def initialize(path)
    puts "initialize(#{path})"

    is_existing_db = File.exist?(path)

    # pn = Pathname.new(path)
    # pp pn
    # realpath = pn.realdirpath
    # pp realpath

    # A Sqlite3 connection to a persistent database
    db_url = "sqlite://#{File.expand_path(path)}"
    puts "db_url: #{db_url}"
    @adapter = DataMapper.setup(:file_info, db_url)

    DataMapper.repository(:file_info) do
      DataMapper.finalize

      if is_existing_db
        puts "Existing DB: upgrading!"
        DataMapper.auto_upgrade!  # adapt existing DB to our needs
      else
        puts "new DB: migrating!"
        DataMapper.auto_migrate!  # drop & creata tables to get empty tables in pristine condition
      end
    end

  end

end
