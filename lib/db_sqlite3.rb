#
# DbSqlite3 class - abstraction to make it easier to use SQLite3 databases within FilePile
#

require 'sqlite3'

class DbSqlite3

  # init new instance
  private
  def initialize(db)
    @db = db
  end

  # close database, if it's open
  public
  def close
    @db.close if @db
  end

  public
  def execute(sql)
    # puts "executing sql: #{sql}"
    @db.execute(sql) if @db
  end


  # 'files' table
  private
  def self.files_table_info
    @files_table_info ||= {
      table_name: 'files',

      # columns in proper order (as SQLite3 knows them)
      # NOTE 'own' (not owner) and 'grp' (not group)
      columns: %w[ id type name size mode mtime own grp sha256 path ],

      # uniqueness constraint columns, there should never be two records that
      # have the same values for all of these columns
      columns_unique: %w[ type name size mode mtime own grp sha256 path ],
      # basically all columns EXCEPT id (since it's just a sequence number)

      # column_info specifies the attribute name, type, value mapping etc.
      # attr_name is the key used when fetching values from an index file (.store)
      columns_info: {
        'id'     => {attr_name: :id, type: :integer, primary_key: true}, # 'id' will be handled in special ways (omitted in SELECT, rewritten in INSERT)
        'type'   => {attr_name: :type, type: :integer, mapping: {:file => 1, :dir => 2}}, # mapping to convert from symbol to integer (integer in db)
        'name'   => {attr_name: :name, type: :text},
        'size'   => {attr_name: :size, type: :integer},
        'mode'   => {attr_name: :mode, type: :text},
        'mtime'  => {attr_name: :mtime, type: :integer},
        'own'    => {attr_name: :owner, type: :text}, # owner -> own
        'grp'    => {attr_name: :group, type: :text}, # group -> grp
        'sha256' => {attr_name: :sha256, type: :text},
        'path'   => {attr_name: :path, type: :text},
      },

    }
  end

  # get the detailed information for a given table
  private
  def table_info_for(table)
    if :files == table
      return DbSqlite3.files_table_info
    else
      return nil
    end
  end

  # create a given table
  public
  def create_table(table)
    table_info = table_info_for(table)
    columns_string = table_info[:columns].map do |column|
      column_info = table_info[:columns_info][column]
      col_def = "#{column} #{column_info[:type].to_s.upcase}"
      col_def += " PRIMARY KEY" if column_info[:primary_key]
      col_def
    end.join(", ")
    table_name = table_info[:table_name]
    sql = "CREATE TABLE IF NOT EXISTS #{table_name}(#{columns_string})" 
    puts "sql: #{sql}"
    execute sql

    # also create an index to make future SELECT statements faster
    columns_string = table_info[:columns_unique].map do |column|
      column_info = table_info[:columns_info][column]
      col_def = "#{column}"
      col_def
    end.join(", ")
    sql = "CREATE INDEX IF NOT EXISTS #{table_name}_index_all_unique_columns ON #{table_name}(#{columns_string})" 
    puts "sql: #{sql}"
    execute sql
  end

  # create a new database
  public
  def self.create_database(path, &block)
    if File.exist? path
      raise "Database file '#{path}' already exist"
    end

    begin
      db_sqlite3 = nil

      # create new database file
      db = SQLite3::Database.new path
      db.results_as_hash = true

      db_sqlite3 = DbSqlite3.new(db)

      # create the 'files' table
      db_sqlite3.create_table(:files)

      # call user's block, if given
      yield(db_sqlite3) if block_given?

      db_sqlite3.close

    rescue SQLite3::Exception => e 
      puts "SQLite3::Exception occured"
      puts e.message
      puts e.backtrace
      db_sqlite3.close

    end
  end


  # open an existing database
  public
  def self.open_database(path, &block)
    unless File.exist? path
      raise "Database file '#{path}' does not exist"
    end

    begin
      db_sqlite3 = nil

      # create new database file
      db = SQLite3::Database.open path
      db.results_as_hash = true

      db_sqlite3 = DbSqlite3.new(db)

      # check if the 'files' table exist
      # TODO: also check that the index exist?
      sql = "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='files';"
      count = db.execute(sql).first[0]
      if count == 0
        raise "Table 'files' does not exist in database '#{path}'"
      end

      # call user's block, if given
      yield(db_sqlite3) if block_given?

      db_sqlite3.close

    rescue SQLite3::Exception => e 
      puts "SQLite3::Exception occured"
      puts e.message
      puts e.backtrace
      db_sqlite3.close if db_sqlite3

    end
  end


  # create a new database
  public
  def self.open_or_create_database(path, &block)
    if File.exist? path
      # open existing database file
      open_database(path) do |db|
        yield(db) if block_given?
      end
    else
      # create new database file
      create_database(path) do |db|
        yield(db) if block_given?
      end
    end
  end


  # count the number of rows in the given table
  public
  def count_rows(table)
    table_info = table_info_for(table)
    table_name = table_info[:table_name]
    sql = "SELECT COUNT(*) FROM #{table_name}" 
    count = execute(sql).first[0]
  end

  public
  def max_id(table)
    table_info = table_info_for(table)
    table_name = table_info[:table_name]
    sql = "SELECT MAX(id) FROM #{table_name}"
    max_id = execute(sql).first[0]
  end

  # Creates a sqlite3 compatible string suitable for use in a WHERE statement (comparing a column to the given value)
  private
  def self.sql_where_value_string(column_name, value, type)
    case type
    when :integer
      "#{column_name}=#{value}"  # integers are not quoted
    when :text
      escaped_value = value.gsub("'", "''") # single-quotes must be doubled in order to be properly understood by SQLite3
      "#{column_name}='#{escaped_value}'" # strings ARE quoted
    else
      raise "Column type '#{type}' is not handled, yet..."
    end
  end

  # Check for existing rows in a table, where all the unique columns match the value of the given row_hash.
  public
  def count_where_row(table, row_hash)
    table_info = table_info_for(table)
    table_name = table_info[:table_name]

    where_string = table_info[:columns_unique].map do |column|
      column_info = table_info[:columns_info][column]
      
      value = row_hash[column]
      
      DbSqlite3.sql_where_value_string(column, value, column_info[:type])
    end.join(" AND ")

    sql = "SELECT COUNT(*) FROM #{table_name} WHERE #{where_string}"
    exist_count = execute(sql).first[0].to_i
  end


  # Check for existing rows in a table, where all the unique columns match the values from the given attribute_hash. 
  #
  # The table_info knows which attribute map to which table column.
  # The table_info may also contain mappings, so that values from the 
  # attribute hash are mapped to different values in the table column
  public
  def count_where_attributes(table, attributes_hash)
    table_info = table_info_for(table)
    table_name = table_info[:table_name]

    where_string = table_info[:columns_unique].map do |column|
      column_info = table_info[:columns_info][column]
      attr_name = column_info[:attr_name]
      
      value = attributes_hash[attr_name]

      if mapping = column_info[:mapping]
        value = mapping[value]
      end

      DbSqlite3.sql_where_value_string(column, value, column_info[:type])
    end.join(" AND ")

    sql = "SELECT COUNT(*) FROM #{table_name} WHERE #{where_string}"
    execute sql
  end

  # Find existing rows in a table, where all the unique columns match the values from the given attribute_hash. 
  #
  # The table_info knows which attribute map to which table column.
  # The table_info may also contain mappings, so that values from the 
  # attribute hash are mapped to different values in the table column
  public
  def where_attributes(table, attributes_hash, ignore_attributes = {})
    table_info = table_info_for(table)
    table_name = table_info[:table_name]

    where_string = table_info[:columns_unique].map do |column|
      column_info = table_info[:columns_info][column]
      attr_name = column_info[:attr_name]

      # caller may want to ignore certain attributes (e.g. columns that it doesn't have data for)
      if ignore_attributes[attr_name]
        # ignore this by emitting 'nil' here, and removing it in reject{} before the join{} (see below)
      else
        value = attributes_hash[attr_name]

        if mapping = column_info[:mapping]
          value = mapping[value]
        end

        DbSqlite3.sql_where_value_string(column, value, column_info[:type])
      end
      
    end.reject{ |ws| ws.nil? }.join(" AND ")

    sql = "SELECT * FROM #{table_name} WHERE #{where_string}"
    execute sql
  end


  # Creates a sqlite3 compatible string suitable for use in an INSERT statement.
  # Integers are not quoted.
  # Strings are quoted in single-quotes, with doubling of actual single-quotes in the value itself (that's how they are escaped)
  private
  def self.sql_insert_value_string(value, type)
    case type
    when :integer
      "#{value}"  # integers are not quoted
    when :text
      escaped_value = value.gsub("'", "''") # single-quotes must be doubled in order to be properly understood by SQLite3
      "'#{escaped_value}'" # strings ARE quoted
    else
      raise "Column type '#{type}' is not handled, yet..."
    end
  end

  # Insert a new row into a table, fetching the values from another row hash.
  public
  def insert_row(table, row_hash)
    table_info = table_info_for(table)
    values_string = table_info[:columns].map do |column|
      column_info = table_info[:columns_info][column]
      
      value = row_hash[column]
      
      DbSqlite3.sql_insert_value_string(value, column_info[:type])
    end.join(",")

    sql = "INSERT INTO files VALUES(#{values_string})"
    # puts "sql: #{sql}"
    execute sql
  end

  # Insert a new row into a table, fetching the values from an attribute hash. 
  #
  # The table_info knows which attribute map to which table column.
  # The table_info may also contain mappings, so that values from the 
  # attribute hash are mapped to different values in the table column
  public
  def insert_attributes(table, attributes_hash)
    table_info = table_info_for(table)
    table_name = table_info[:table_name]

    values_string = table_info[:columns].map do |column|
      column_info = table_info[:columns_info][column]
      attr_name = column_info[:attr_name]
      
      value = attributes_hash[attr_name]

      if mapping = column_info[:mapping]
        value = mapping[value.to_sym]
      end

      sql_insert_value_string(value, column_info[:type])
    end.join(",")

    sql = "INSERT INTO #{table_name} VALUES(#{values_string})"
    # puts "sql: #{sql}"
    execute sql
  end

  # create a key string from the attributes of a row in the 'files' table
  # the key string will contain attribute values separated by the '+' character
  private
  def self.calculate_key_string_for_files_row(table, row)
    table_info = table_info_for(table)
    key_string = table_info[:columns_unique].map{|attr| row[attr]}.join('+')
  end

  # create a hash with col=>value pairs, from a row in the 'files' table
  private
  def self.create_hash_for_files_row(table, row)
    table_info = table_info_for(table)
    Hash[* table_info[:columns_unique].map{|col| [col, row[col]]}.flatten]
  end

  # read all rows from the 'files' table and store them in a hash
  # if a row already exist in the hash, then it is skipped
  public
  def import_files_into_hash(table, unique_files_hash)
    table_info = table_info_for(table)
    # get all the records from db
    sql = "SELECT * FROM files" 
    rows = execute sql
    puts "file records: #{rows.size}"

    num_added = 0
    num_skipped = 0

    # process each row, insert into unique_files_hash hash unless the hash already contains that entry
    rows.each do |row|
      key_string = calculate_key_string_for_files_row(table_info, row)
      # key = StringHash.sha256(key_string)
      unless unique_files_hash.key? key_string
        # add row to hash
        unique_files_hash[key_string] = create_hash_for_files_row(table_info, row)
        num_added += 1
      else
        num_skipped += 1
      end
    end
    puts "import summary:"
    puts "  files added: #{num_added}"
    puts "  files skipped: #{num_skipped}"
  end

end