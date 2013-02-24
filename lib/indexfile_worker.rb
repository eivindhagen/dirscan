require File.join(File.dirname(__FILE__), 'hasher')
require File.join(File.dirname(__FILE__), 'pathinfo')
require File.join(File.dirname(__FILE__), 'indexfile')
require File.join(File.dirname(__FILE__), 'pipeline')

require 'pathname'
require 'json'
require 'bindata'
require 'socket'

class IndexFileWorker < Worker
  attr_accessor :inputs, :outputs

  # unpack a binary index file and write a text version of the file
  def unpack(options = {})
    required_input_files :scan_index
    required_output_files :scan_unpack

    # read the index file
    IndexFile::Reader.new(input_file(:scan_index)) do |index_file|
      # write the text file (UTF-8 encoding)
      options = {
        external_encoding: Encoding::UTF_8,
      }
      File.open(output_file(:scan_unpack), 'w', options) do |unpack_file|
        while not index_file.eof? do
          object = index_file.read_object
          unpack_file.write(JSON.pretty_generate(object) + "\n")
        end
      end
    end
  end

  # exports a CSV file from a binary index file
  def export_csv(options = {})
    required_input_files :index_path
    required_output_files :csv_path

    object_types_to_export = %w[ file symlink ]
    columns_to_export = %w[ type name size mode mtime owner group sha256 path ].map{|col| col.to_sym} # we want symbols (not strings)

    last_dir = nil

    # read the index file
    IndexFile::Reader.new(input_file(:index_path)) do |index_file|
      # write the text file
      File.open(output_file(:csv_path), 'w') do |csv_file|
        csv_file.write columns_to_export.join(', ') + "\n"

        while not index_file.eof? do
          object = index_file.read_object

          # track last 'dir' entry
          if 'dir' == object[:type]
            last_dir = object
          end

          # write to CSV file
          if object_types_to_export.include? object[:type]
            # the path comes from the more recent 'dir' entry
            object[:path] = last_dir[:path]

            values = columns_to_export.map{|col| object[col]}
            csv_file.write values.join(', ') + "\n"
          end

        end # while
      end # File.open
    end # IndexFile::Reader
  end


  #
  # sqlite3 methods
  #

  require 'sqlite3'

  # 'files' table
  def files_table_info
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
        'type'   => {attr_name: :type, type: :integer, mapping: {'file' => 1, 'dir' => 2}}, # mapping to convert from string to integer (integer in db)
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

  # create a given table
  def create_table(db, table_info)
    columns_string = table_info[:columns].map do |column|
      column_info = table_info[:columns_info][column]
      col_def = "#{column} #{column_info[:type].to_s.upcase}"
      col_def += " PRIMARY KEY" if column_info[:primary_key]
      col_def
    end.join(", ")
    table_name = table_info[:table_name]
    sql = "CREATE TABLE IF NOT EXISTS #{table_name}(#{columns_string})" 
    puts "sql: #{sql}"
    db.execute sql

    # also create an index to make future SELECT statements faster
    columns_string = table_info[:columns_unique].map do |column|
      column_info = table_info[:columns_info][column]
      col_def = "#{column}"
      col_def
    end.join(", ")
    sql = "CREATE INDEX IF NOT EXISTS #{table_name}_index_all_unique_columns ON #{table_name}(#{columns_string})" 
    puts "sql: #{sql}"
    db.execute sql
  end


  # count the number of rows in the given table
  def count_rows(db, table_info)
    table_name = table_info[:table_name]
    sql = "SELECT COUNT(*) FROM #{table_name}" 
    count = db.execute(sql).first[0]
  end

  def max_id(db, table_info)
    table_name = table_info[:table_name]
    sql = "SELECT MAX(id) FROM #{table_name}"
    max_id = db.execute(sql).first[0]
  end

  # Creates a sqlite3 compatible string suitable for use in a WHERE statement (comparing a column to the given value)
  def sql_where_value_string(column_name, value, type)
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

  # Find an existing row in a table, where all the unique columns match the value of the given row_hash.
  def count_where_row(db, table_info, row_hash)
    table_name = table_info[:table_name]

    where_string = table_info[:columns_unique].map do |column|
      column_info = table_info[:columns_info][column]
      
      value = row_hash[column]
      
      sql_where_value_string(column, value, column_info[:type])
    end.join(" AND ")

    sql = "SELECT COUNT(*) FROM #{table_name} WHERE #{where_string}"
    # puts "sql: #{sql}"
    exist_count = db.execute(sql).first[0].to_i
  end


  # Creates a sqlite3 compatible string suitable for use in an INSERT statement.
  # Integers are not quoted.
  # Strings are quoted in single-quotes, with doubling of actual single-quotes in the value itself (that's how they are escaped)
  def sql_insert_value_string(value, type)
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
  def insert_row(db, table_info, row_hash)
    values_string = table_info[:columns].map do |column|
      column_info = table_info[:columns_info][column]
      
      value = row_hash[column]
      
      sql_insert_value_string(value, column_info[:type])
    end.join(",")

    sql = "INSERT INTO files VALUES(#{values_string})"
    # puts "sql: #{sql}"
    db.execute sql
  end

  # Insert a new row into a table, fetching the values from an attribute hash. 
  # The table_info knows which attribute map to which table column.
  # The table_info may also contain mappings, so that values from the 
  # attribute hash are mapped to different values in the table column
  def insert_attributes(db, table_info, attributes_hash)
    values_string = table_info[:columns].map do |column|
      column_info = table_info[:columns_info][column]
      attr_name = column_info[:attr_name]
      
      value = attributes_hash[attr_name]

      if mapping = column_info[:mapping]
        value = mapping[value]
      end

      sql_insert_value_string(value, column_info[:type])
    end.join(",")

    sql = "INSERT INTO files VALUES(#{values_string})"
    # puts "sql: #{sql}"
    db.execute sql
  end

  # create a key string from the attributes of a row in the 'files' table
  # the key string will contain attribute values separated by the '+' character
  def calculate_key_string_for_files_row(table_info, row)
    key_string = table_info[:columns_unique].map{|attr| row[attr]}.join('+')
  end

  # create a hash with col=>value pairs, from a row in the 'files' table
  def create_hash_for_files_row(table_info, row)
    Hash[* table_info[:columns_unique].map{|col| [col, row[col]]}.flatten]
  end

  # read all rows from the 'files' table and store them in a hash
  # if a row already exist in the hash, then it is skipped
  def import_files_into_hash(db, table_info, unique_files_hash)
    # get all the records from db
    sql = "SELECT * FROM files" 
    rows = db.execute sql
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

  #
  # sqlite3 worker methods
  #

  # create an empty sqlite3 database
  def create_sqlite3(options = {})
    required_output_files :db_path

    begin
      # create the database file
      db = SQLite3::Database.new output_file(:db_path)
      # create the 'files' table
      create_table(db, files_table_info)
    
    rescue SQLite3::Exception => e 
      puts "SQLite3::Exception occured"
      puts e.message
      puts e.backtrace

    ensure
      db.close if db

    end
  end

  # inspect a sqlite3 database
  def inspect_sqlite3(options = {})
    required_input_files :db_path

    begin
      # create the database file
      db = SQLite3::Database.open input_file(:db_path)

      files_count = count_rows(db, files_table_info)
      puts "files_count: #{files_count}"
    
    rescue SQLite3::Exception => e 
      puts "SQLite3::Exception occured"
      puts e.message
      puts e.backtrace

    ensure
      db.close if db

    end
  end

  # exports a sqlite3 file from a binary index file
  def export_sqlite3(options = {})
    required_input_files :index_path
    required_output_files :db_path

    object_types_to_export = %w[ file ] # TODO: add 'dir' and 'symlink'

    last_dir = nil

    # read the index file
    IndexFile::Reader.new(input_file(:index_path)) do |index_file|
      begin
        # create the database file
        db_out = SQLite3::Database.new(output_file(:db_path))
        # create the 'files' table
        create_table(db_out, files_table_info)

        db_out.execute "BEGIN" # start transactions (for better performance)

        record_id = 1
        while not index_file.eof? do
          object = index_file.read_object

          # track last 'dir' entry, for access to the current 'path'
          if 'dir' == object[:type]
            last_dir = object
          end

          # insert into sqlite3 database
          if object_types_to_export.include? object[:type]

            # the path comes from the morstrecent 'dir' entry
            object[:path] = last_dir[:path]

            # merge in the sequential record id
            insert_attributes(db_out, files_table_info, object.merge(id: record_id))
            record_id += 1 # increment for next record
          end
          
        end # while
        db_out.execute "COMMIT" # commit transactions (for better performance)
        puts "record_id after last write: #{record_id}"
      
      rescue SQLite3::Exception => e 
        puts "SQLite3::Exception occured"
        puts e.message
        puts e.backtrace

      ensure
        db_out.close if db_out

      end
    end # IndexFile::Reader
  end


  require "benchmark"
 
  # merge two sqlite3 databases and output a single sqlite3 database
  # uses that largest database as the startign point, then adds only new records from the smaller db to the larger one
  #
  # Performance profile:
  # - Slow, because identification of existing (duplicate) records is done using SQL SELECT statement
  # - Uses very little memory, because records are processed and then forgotten, it's the DB's job to find duplicates for us (and that's why it's slow)
  # - Suitable for when the combined size of the in_db's is larger than available memory, especially if one in_db is very large and the other in_db is very small
  def merge_sqlite3(options = {})
    required_input_files :db_in1_path, :db_in2_path
    required_output_files :db_out_path

    # first copy the largest db_in file to db_out, then merge the smaller db_in file to db_out
    db_in1_path = input_file(:db_in1_path)
    db_in2_path = input_file(:db_in2_path)
    db_out_path = output_file(:db_out_path)

    # determine which input file is large / small
    # it's faster to merge the small file into the large files, because there are fewer records to transfer
    if File.size(db_in1_path) > File.size(db_in2_path)
      db_in_large_path = db_in1_path
      db_in_small_path = db_in2_path
    else
      db_in_large_path = db_in2_path
      db_in_small_path = db_in1_path
    end

    # copy the large file to db_out
    FileUtils.copy_file(db_in_large_path, db_out_path)

    db_in = nil
    db_out = nil

    begin
      # open db_in_small so we can read from it
      db_in = SQLite3::Database.open(db_in_small_path)
      db_in.results_as_hash = true

      # open db_out so we can write to it
      db_out = SQLite3::Database.open(db_out_path)
      db_out.results_as_hash = true

      # see how many rows are in the 'files' table of each db
      db_in_count = count_rows(db_in, files_table_info)
      db_out_count = count_rows(db_out, files_table_info)
      puts "db_in has #{db_in_count} rows"
      puts "db_out has #{db_out_count} rows"

      # find the largest 'id' in db_out
      db_out_id_max = max_id(db_out, files_table_info)
      puts "db_out MAX(id): #{db_out_id_max}"

      # get all the records from db_in
      # columns_string = columns_to_merge.join(',')
      # sql = "SELECT #{columns_string} FROM files" 
      sql = "SELECT * FROM files" 
      # puts "sql: #{sql}"
      in_rows = db_in.execute sql
      puts "rows to merge: #{in_rows.count}"
      num_added = 0
      num_skipped = 0

      db_out.execute "BEGIN" # start transactions (for better performance)

      next_id = db_out_id_max + 1
      in_rows.each do |in_row|
        # see if the in_row already exists in db_out
        exist_count = false

        time = Benchmark.measure do
          exist_count = count_where_row(db_out, files_table_info, in_row)
        end
        # puts "time = #{time}"

        # if in_row does not exist in db_out, then instert it into db_out
        if exist_count == 0
          insert_row(db_out, files_table_info, in_row.merge({'id' => next_id}))
          next_id += 1
          num_added += 1
        else
          num_skipped += 1
        end
      end

      db_out.execute "COMMIT" # commit transactions (for better performance)
      puts "num_added: #{num_added}"
      puts "num_skipped: #{num_skipped}"

    rescue SQLite3::Exception => e 
      puts "SQLite3::Exception occured"
      puts e.message
      puts e.backtrace

    ensure
      db_in.close if db_in
      db_out.close if db_out

    end
  end


  # Merge two sqlite3 databases and output a single sqlite3 database with all unique records (duplicates removed).
  # Uses that largest database as the starting point, then adds only new records from the smaller db to the larger one.
  # Uses ruby hash to speed up the detection of duplicates.
  #
  # Performance profile:
  # - Fastest, because identification of existing (duplicate) records is done with a Ruby Hash AND because we're appending to an existing DB (adding db_in_smallest to db_in_largest)
  # - Uses a lot of memory, because ALL records are imported into memory and stored in the Roby Hash
  # - Suitable for when the input databases combined size is less than available memory
  def merge_sqlite3_hybrid(options = {})
    required_input_files :db_in1_path, :db_in2_path
    required_output_files :db_out_path

    # first copy the largest db_in file to db_out, then merge the smaller db_in file to db_out
    db_in1_path = input_file(:db_in1_path)
    db_in2_path = input_file(:db_in2_path)
    db_out_path = output_file(:db_out_path)

    # determine which input file is larger / smaller
    # it's faster to merge the small file into the large files, because there is less data to add
    if File.size(db_in1_path) > File.size(db_in2_path)
      db_in_large_path = db_in1_path
      db_in_small_path = db_in2_path
    else
      db_in_large_path = db_in2_path
      db_in_small_path = db_in1_path
    end

    # copy the large file to db_out
    FileUtils.copy_file(db_in_large_path, db_out_path)

    #
    # merge small db_in to db_out, by reading all records and adding them to db_out (unless the record already exist)
    #

    db_in = nil
    db_out = nil

    begin
      # open db_in_small so we can read from it
      db_in = SQLite3::Database.open(db_in_small_path)
      db_in.results_as_hash = true

      # open db_out so we can write to it
      db_out = SQLite3::Database.open(db_out_path)
      db_out.results_as_hash = true

      # read all existing records of db_out into a hash (this hash will be used for fast lookup later, to avoid adding duplicate entries)
      unique_files = {}
      import_files_into_hash(db_out, files_table_info, unique_files)

      # see how many rows are in the 'files' table of each db
      db_in_count = count_rows(db_in, files_table_info)
      db_out_count = count_rows(db_out, files_table_info)
      puts "db_in has #{db_in_count} rows"
      puts "db_out has #{db_out_count} rows"

      # find the largest 'id' in db_out
      db_out_id_max = max_id(db_out, files_table_info)
      puts "db_out MAX(id): #{db_out_id_max}"

      # get all the records from db_in
      # columns_string = columns_to_merge.join(',')
      # sql = "SELECT #{columns_string} FROM files" 
      sql = "SELECT * FROM files"
      # puts "sql: #{sql}"
      in_rows = db_in.execute sql
      puts "rows to merge: #{in_rows.count}"
      num_added = 0
      num_skipped = 0

      db_out.execute "BEGIN" # start transactions (for better performance)

      next_id = db_out_id_max + 1
      in_rows.each do |in_row|
        # see if the in_row already exists in db_out
        key_string = calculate_key_string_for_files_row(files_table_info, in_row)

        # if in_row does not exist in db_out, then insert it into db_out
        unless unique_files.key? key_string
          unique_files[key_string] = create_hash_for_files_row(files_table_info, in_row)

          insert_row(db_out, files_table_info, in_row.merge({'id' => next_id}))
          next_id += 1
          num_added += 1
        else
          num_skipped += 1
        end
      end

      db_out.execute "COMMIT" # commit transactions (for better performance)
      puts "merge summary:"
      puts "  num_added: #{num_added}"
      puts "  num_skipped: #{num_skipped}"

    rescue SQLite3::Exception => e 
      puts "SQLite3::Exception occured"
      puts e.message
      puts e.backtrace

    ensure
      db_in.close if db_in
      db_out.close if db_out

    end
  end


  # Merge two sqlite3 databases and output a single sqlite3 database (without any duplicated records).
  # Uses ruby hash to collect unique records, then writes out a brand new database from that hash.
  #
  # Performance profile:
  # - Quite fast, because identification of existing (duplicate) records is done with a Ruby Hash
  # - Slower than the 'hybrid' implementation, because db_out is recreated from scratch (whereas 'hybrid' appends the new records from db_in_smallest to db_in_largest)
  # - Uses a lot of memory, because ALL records are imported into memory and stored in the Roby Hash
  # - Suitable for when the input databases combined size is less than available memory
  def merge_sqlite3_fast(options = {})
    required_input_files :db_in1_path, :db_in2_path
    required_output_files :db_out_path

    db_in1_path = input_file(:db_in1_path)
    db_in2_path = input_file(:db_in2_path)
    db_out_path = output_file(:db_out_path)

    # build a hash of all records, indexed by a checksum of the uniqueness attributes
    unique_files = {}

    #
    # read both files and add files to the unique_files hash
    #

    [db_in1_path, db_in2_path].each do |db_in_path|
      begin
        # create db_out so we can write to it
        db_in = SQLite3::Database.open(db_in_path)
        db_in.results_as_hash = true

        # read all records (duplicates are ignored)
        import_files_into_hash(db_in, files_table_info, unique_files)
      rescue SQLite3::Exception => e 
        puts "SQLite3::Exception occured"
        puts e.message
        puts e.backtrace

      ensure
        db_in.close if db_in

      end
    end

    #
    # write the unique_files to db_out
    #
    begin

      # create db_out so we can write to it
      db_out = SQLite3::Database.new(db_out_path)
      db_out.results_as_hash = true

      create_table(db_out, files_table_info)

      result = db_out.execute "BEGIN" # transactions make this much faster

      puts "file records to write: #{unique_files.count}"
      next_id = 1
      unique_files.each do |key, row_hash|
        insert_row(db_out, files_table_info, row_hash.merge({'id' => next_id}))
        next_id += 1
      end
      puts "next_id after last write: #{next_id}"
        
    rescue SQLite3::Exception => e 
      puts "SQLite3::Exception occured"
      puts e.message
      puts e.backtrace

    ensure
      result = db_out.execute "COMMIT" # transactions make this much faster

      db_out.close if db_out

    end
  end # def merge_sqlite3_fast

end
