require File.join(File.dirname(__FILE__), 'hasher')
require File.join(File.dirname(__FILE__), 'pathinfo')
require File.join(File.dirname(__FILE__), 'indexfile')
require File.join(File.dirname(__FILE__), 'pipeline')
require File.join(File.dirname(__FILE__), 'db_sqlite3')

require 'pathname'
require 'json'
require 'bindata'
require 'socket'
require "benchmark"

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


  # #
  # # sqlite3 methods
  # #

  # require 'sqlite3'


  # # 'files' table
  # def files_table_info
  #   @files_table_info ||= {
  #     table_name: 'files',

  #     # columns in proper order (as SQLite3 knows them)
  #     # NOTE 'own' (not owner) and 'grp' (not group)
  #     columns: %w[ id type name size mode mtime own grp sha256 path ],

  #     # uniqueness constraint columns, there should never be two records that
  #     # have the same values for all of these columns
  #     columns_unique: %w[ type name size mode mtime own grp sha256 path ],
  #     # basically all columns EXCEPT id (since it's just a sequence number)

  #     # column_info specifies the attribute name, type, value mapping etc.
  #     # attr_name is the key used when fetching values from an index file (.store)
  #     columns_info: {
  #       'id'     => {attr_name: :id, type: :integer, primary_key: true}, # 'id' will be handled in special ways (omitted in SELECT, rewritten in INSERT)
  #       'type'   => {attr_name: :type, type: :integer, mapping: {'file' => 1, 'dir' => 2}}, # mapping to convert from string to integer (integer in db)
  #       'name'   => {attr_name: :name, type: :text},
  #       'size'   => {attr_name: :size, type: :integer},
  #       'mode'   => {attr_name: :mode, type: :text},
  #       'mtime'  => {attr_name: :mtime, type: :integer},
  #       'own'    => {attr_name: :owner, type: :text}, # owner -> own
  #       'grp'    => {attr_name: :group, type: :text}, # group -> grp
  #       'sha256' => {attr_name: :sha256, type: :text},
  #       'path'   => {attr_name: :path, type: :text},
  #     },

  #   }
  # end

  # # create a given table
  # def create_table(db, table_info)
  #   columns_string = table_info[:columns].map do |column|
  #     column_info = table_info[:columns_info][column]
  #     col_def = "#{column} #{column_info[:type].to_s.upcase}"
  #     col_def += " PRIMARY KEY" if column_info[:primary_key]
  #     col_def
  #   end.join(", ")
  #   table_name = table_info[:table_name]
  #   sql = "CREATE TABLE IF NOT EXISTS #{table_name}(#{columns_string})" 
  #   puts "sql: #{sql}"
  #   db.execute sql

  #   # also create an index to make future SELECT statements faster
  #   columns_string = table_info[:columns_unique].map do |column|
  #     column_info = table_info[:columns_info][column]
  #     col_def = "#{column}"
  #     col_def
  #   end.join(", ")
  #   sql = "CREATE INDEX IF NOT EXISTS #{table_name}_index_all_unique_columns ON #{table_name}(#{columns_string})" 
  #   puts "sql: #{sql}"
  #   db.execute sql
  # end


  # # create a new database
  # def create_database(path)
  #   begin
  #     if File.exist? path
  #       raise "Database file '#{path}' already exist"
  #     end

  #     # create new database file
  #     db = SQLite3::Database.new path
  #     db.results_as_hash = true

  #     # create the 'files' table
  #     create_table(db, files_table_info)
  #     return db

  #   rescue SQLite3::Exception => e 
  #     puts "SQLite3::Exception occured"
  #     puts e.message
  #     puts e.backtrace
  #     db.close if db
  #     return nil

  #   end
  # end


  # # open an existing database
  # def open_database(path)
  #   begin
  #     unless File.exist? path
  #       raise "Database file '#{path}' does not exist"
  #     end

  #     # create new database file
  #     db = SQLite3::Database.open path
  #     db.results_as_hash = true

  #     # check if the 'files' table exist
  #     # TODO: also check that the index exist?
  #     sql = "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='files';"
  #     count = db.execute(sql).first[0]
  #     if count == 0
  #       raise "Table 'files' does not exist in database '#{path}'"
  #     end

  #     return db

  #   rescue SQLite3::Exception => e 
  #     puts "SQLite3::Exception occured"
  #     puts e.message
  #     puts e.backtrace
  #     db.close if db
  #     return nil

  #   end
  # end


  # # create a new database
  # def open_or_create_database(path)
  #   if File.exist? path
  #     # open existing database file
  #     open_database(path)
  #   else
  #     # create new database file
  #     create_database(path)
  #   end
  # end


  # # count the number of rows in the given table
  # def count_rows(db, table_info)
  #   table_name = table_info[:table_name]
  #   sql = "SELECT COUNT(*) FROM #{table_name}" 
  #   count = db.execute(sql).first[0]
  # end

  # def max_id(db, table_info)
  #   table_name = table_info[:table_name]
  #   sql = "SELECT MAX(id) FROM #{table_name}"
  #   max_id = db.execute(sql).first[0]
  # end

  # # Creates a sqlite3 compatible string suitable for use in a WHERE statement (comparing a column to the given value)
  # def sql_where_value_string(column_name, value, type)
  #   case type
  #   when :integer
  #     "#{column_name}=#{value}"  # integers are not quoted
  #   when :text
  #     escaped_value = value.gsub("'", "''") # single-quotes must be doubled in order to be properly understood by SQLite3
  #     "#{column_name}='#{escaped_value}'" # strings ARE quoted
  #   else
  #     raise "Column type '#{type}' is not handled, yet..."
  #   end
  # end

  # # Find an existing row in a table, where all the unique columns match the value of the given row_hash.
  # def count_where_row(db, table_info, row_hash)
  #   table_name = table_info[:table_name]

  #   where_string = table_info[:columns_unique].map do |column|
  #     column_info = table_info[:columns_info][column]
      
  #     value = row_hash[column]
      
  #     sql_where_value_string(column, value, column_info[:type])
  #   end.join(" AND ")

  #   sql = "SELECT COUNT(*) FROM #{table_name} WHERE #{where_string}"
  #   # puts "sql: #{sql}"
  #   exist_count = db.execute(sql).first[0].to_i
  # end


  # # Creates a sqlite3 compatible string suitable for use in an INSERT statement.
  # # Integers are not quoted.
  # # Strings are quoted in single-quotes, with doubling of actual single-quotes in the value itself (that's how they are escaped)
  # def sql_insert_value_string(value, type)
  #   case type
  #   when :integer
  #     "#{value}"  # integers are not quoted
  #   when :text
  #     escaped_value = value.gsub("'", "''") # single-quotes must be doubled in order to be properly understood by SQLite3
  #     "'#{escaped_value}'" # strings ARE quoted
  #   else
  #     raise "Column type '#{type}' is not handled, yet..."
  #   end
  # end

  # # Insert a new row into a table, fetching the values from another row hash.
  # def insert_row(db, table_info, row_hash)
  #   values_string = table_info[:columns].map do |column|
  #     column_info = table_info[:columns_info][column]
      
  #     value = row_hash[column]
      
  #     sql_insert_value_string(value, column_info[:type])
  #   end.join(",")

  #   sql = "INSERT INTO files VALUES(#{values_string})"
  #   # puts "sql: #{sql}"
  #   db.execute sql
  # end

  # # Insert a new row into a table, fetching the values from an attribute hash. 
  # # The table_info knows which attribute map to which table column.
  # # The table_info may also contain mappings, so that values from the 
  # # attribute hash are mapped to different values in the table column
  # def insert_attributes(db, table_info, attributes_hash)
  #   values_string = table_info[:columns].map do |column|
  #     column_info = table_info[:columns_info][column]
  #     attr_name = column_info[:attr_name]
      
  #     value = attributes_hash[attr_name]

  #     if mapping = column_info[:mapping]
  #       value = mapping[value]
  #     end

  #     sql_insert_value_string(value, column_info[:type])
  #   end.join(",")

  #   sql = "INSERT INTO files VALUES(#{values_string})"
  #   # puts "sql: #{sql}"
  #   db.execute sql
  # end

  # # create a key string from the attributes of a row in the 'files' table
  # # the key string will contain attribute values separated by the '+' character
  # def calculate_key_string_for_files_row(table_info, row)
  #   key_string = table_info[:columns_unique].map{|attr| row[attr]}.join('+')
  # end

  # # create a hash with col=>value pairs, from a row in the 'files' table
  # def create_hash_for_files_row(table_info, row)
  #   Hash[* table_info[:columns_unique].map{|col| [col, row[col]]}.flatten]
  # end

  # # read all rows from the 'files' table and store them in a hash
  # # if a row already exist in the hash, then it is skipped
  # def import_files_into_hash(db, table_info, unique_files_hash)
  #   # get all the records from db
  #   sql = "SELECT * FROM files" 
  #   rows = db.execute sql
  #   puts "file records: #{rows.size}"

  #   num_added = 0
  #   num_skipped = 0

  #   # process each row, insert into unique_files_hash hash unless the hash already contains that entry
  #   rows.each do |row|
  #     key_string = calculate_key_string_for_files_row(table_info, row)
  #     # key = StringHash.sha256(key_string)
  #     unless unique_files_hash.key? key_string
  #       # add row to hash
  #       unique_files_hash[key_string] = create_hash_for_files_row(table_info, row)
  #       num_added += 1
  #     else
  #       num_skipped += 1
  #     end
  #   end
  #   puts "import summary:"
  #   puts "  files added: #{num_added}"
  #   puts "  files skipped: #{num_skipped}"
  # end

  #
  # sqlite3 worker methods
  #

  # create an empty sqlite3 database
  def create_sqlite3(options = {})
    required_output_files :db_path

    # create the database file
    DbSqlite3.create_database(output_file(:db_path))
  end

  # inspect a sqlite3 database
  def inspect_sqlite3(options = {})
    required_input_files :db_path

    # open the database file
    DbSqlite3.open_database(input_file(:db_path)) do |db|
      # count the number of rows in the 'files' table
      files_count = db.count_rows(files_table_info)
      puts "files_count: #{files_count}"
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
      # create the database file
      DbSqlite3.create_database(output_file(:db_path)) do |db_out|
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
            db_out.insert_attributes(files_table_info, object.merge(id: record_id))
            record_id += 1 # increment for next record
          end
          
        end # while
        db_out.execute "COMMIT" # commit transactions (for better performance)
        puts "record_id after last write: #{record_id}"
      end
    end # IndexFile::Reader
  end

 
  # diff two sqlite3 databases and report the differences & commonalities
  #
  # Performance profile:
  # - TODO
  def diff_sqlite3(options = {})
    required_input_values :diff_operation
    required_input_files :db_in1_path, :db_in2_path
    required_output_files :db_out_path

    diff_operation = input_value(:diff_operation)
    unless ['unique', 'common'].include? diff_operation
      raise "Unknown diff operation '#{diff_operation}'"
    end

    db_in1_path = input_file(:db_in1_path)
    db_in2_path = input_file(:db_in2_path)
    db_out_path = output_file(:db_out_path)

    db_in1 = nil
    db_in2 = nil
    db_out = nil

    # open db_in_small so we can read from it
    DbSqlite3.open_database(db_in1_path) do |db_in1|

      # see how many rows are in the 'files' table
      db_in1_count = db_in1.count_rows(:files)
      puts "db_in1 has #{db_in1_count} rows"

      # get all the records from db_in1
      sql = "SELECT * FROM files" 
      in1_rows = db_in1.execute sql
      puts "rows to diff: #{in1_rows.count}"

      # open db_in2 so we can read from it
      DbSqlite3.open_database(db_in2_path) do |db_in2|

        # see how many rows are in the 'files' table
        db_in2_count = db_in2.count_rows(:files)
        puts "db_in2 has #{db_in2_count} rows"

        # open db_out so we can write to it
        DbSqlite3.create_database(db_out_path) do |db_out|

          num_unique = 0
          num_common = 0
          db_out.execute "BEGIN" # start transactions (for better performance)
          next_id = 1

          in1_rows.each do |in1_row|
            # see if the in1_row exists in db_in2
            exist_count = db_in2.count_where_row(:files, in1_row)

            # if in_row does not exist in db_out, then instert it into db_out
            if exist_count == 0
              if 'unique' == diff_operation
                db_out.insert_row(:files, in1_row.merge({'id' => next_id}))
                next_id += 1
              end
              num_unique += 1
            else
              if 'common' == diff_operation
                db_out.insert_row(:files, in1_row.merge({'id' => next_id}))
                next_id += 1
              end
              num_common += 1
            end
          end

          db_out.execute "COMMIT" # commit transactions (for better performance)
          puts "num_unique: #{num_unique}"
          puts "num_common: #{num_common}"
        end # db_out
      end # db_in2
    end # db_in1

  end


  # merge two sqlite3 databases and output a single sqlite3 database
  # uses that largest database as the startign point, then adds only new records from the smaller db to the larger one
  #
  # Performance profile:
  # - Fast, because SQL SELECT is fast when there is an INDEX on the 'files' table
  # - Uses very little memory, because records are processed and then forgotten, it's the DB's job to find duplicates for us (and that's why it's slow)
  # - Suitable for all cases, and especially if one in_db is very large and the other in_db is very small
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

    # open db_in_small so we can read from it
    DbSqlite3.open_database(db_in_small_path) do |db_in|

      # see how many rows are in the 'files' table
      db_in_count = db_in.count_rows(:files)
      puts "db_in has #{db_in_count} rows"

      # open db_in_large so we can read from it (to check if a record already exist or not)
      DbSqlite3.open_database(db_in_large_path) do |db_in_large|

        # open db_out so we can write to it
        DbSqlite3.open_database(db_out_path) do |db_out|

          # see how many rows are in the 'files' table
          db_out_count = db_out.count_rows(:files)
          puts "db_out has #{db_out_count} rows"

          # find the largest 'id' in db_out
          db_out_id_max = db_out.max_id(:files)
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
            # see if the in_row already exists
            exist_count = db_in_large.count_where_row(:files, in_row)

            # if in_row does not exist, then instert it
            if exist_count == 0
              db_out.insert_row(:files, in_row.merge({'id' => next_id}))
              next_id += 1
              num_added += 1
            else
              num_skipped += 1
            end
          end

          db_out.execute "COMMIT" # commit transactions (for better performance)
          puts "num_added: #{num_added}"
          puts "num_skipped: #{num_skipped}"
        end # db_out
      end # db_in_large
    end # db_in

  end


end # class IndexFileWorker
