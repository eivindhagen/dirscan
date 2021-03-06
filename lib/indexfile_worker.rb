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
      # write the text file
      File.open(output_file(:scan_unpack), 'w') do |unpack_file|
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

  def create_files_table(db)
    db_columns = [
      {key: :id,     name: 'id',     type: 'INTEGER', extra: 'PRIMARY KEY'},
      {key: :type,   name: 'type',   type: 'INTEGER', mapping: {'file' => 1, 'dir' => 2}},
      {key: :name,   name: 'name',   type: 'TEXT'},
      {key: :size,   name: 'size',   type: 'INTEGER'},
      {key: :mode,   name: 'mode',   type: 'TEXT'}, # even though it's an integer, we usually think of this as a string, since each character maps to user/group/other
      {key: :mtime,  name: 'mtime',  type: 'INTEGER'},
      {key: :owner,  name: 'own',    type: 'TEXT'},
      {key: :group,  name: 'grp',    type: 'TEXT'},
      {key: :sha256, name: 'sha256', type: 'TEXT'},
      {key: :path,   name: 'path',   type: 'TEXT'},
    ]

    # create table
    columns_string = db_columns.map{|col| "#{col[:name]} #{col[:type]} #{col[:extra]}"}.join(", ")
    sql = "CREATE TABLE IF NOT EXISTS files(#{columns_string})" 
    # puts "sql: #{sql}"
    db.execute sql
  end


  # count the number of rows in the 'files' table
  def count_files(db)
    # see how many rows are in the table
    sql = "SELECT COUNT(*) FROM files" 
    count = db.execute(sql).first[0]
  end


  # array of all the attributes that make a row in the 'files' table unique (constraint)
  def files_uniqueness_attributes
    @files_uniqueness_attributes ||= %w[ type name size mode mtime own grp sha256 path ] # NOT id (just a sequence number)
  end


  # create a key string from the attributes of a row in the 'files' table
  # the key string will contain attribute values separated by the '+' character
  def calculate_key_string_for_files_row(row)
    files_uniqueness_attributes.map{|attr| row[attr]}.join('+')
  end

  # create a hash with attr=>value pairs, from a row in the 'files' table
  def create_hash_for_files_row(row)
    Hash[* files_uniqueness_attributes.map{|attr| [attr, row[attr]]}.flatten]
  end

  # read all rows from the 'files' table and store them in a hash
  # if a row already exist in the hash, then it is skipped
  def import_files_into_hash(db, unique_files_hash)
    # get all the records from db
    sql = "SELECT * FROM files" 
    rows = db.execute sql
    puts "file records: #{rows.size}"

    num_added = 0
    num_skipped = 0

    # process each row, insert into unique_files_hash hash unless the hash already contains that entry
    rows.each do |row|
      key_string = calculate_key_string_for_files_row(row)
      # key = StringHash.sha256(key_string)
      unless unique_files_hash.key? key_string
        # add row to hash
        unique_files_hash[key_string] = create_hash_for_files_row(row)
        num_added += 1
      else
        num_skipped += 1
      end
    end
    puts "files added: #{num_added}"
    puts "files skipped: #{num_skipped}"
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

      create_files_table(db)
    
    rescue SQLite3::Exception => e 
      puts "Exception occured"
      puts e

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

      files_count = count_files(db)
      puts "files_count: #{files_count}"
    
    rescue SQLite3::Exception => e 
      puts "Exception occured"
      puts e

    ensure
      db.close if db

    end
  end

  # exports a sqlite3 file from a binary index file
  def export_sqlite3(options = {})
    required_input_files :index_path
    required_output_files :db_path

    object_types_to_export = %w[ file symlink ]
    columns_to_export = %w[ type name size mode mtime owner group sha256 path ].map{|col| col.to_sym} # we want symbols (not strings)
    db_columns = [
      {key: :id,     name: 'id',     type: 'INTEGER', extra: 'PRIMARY KEY'},
      {key: :type,   name: 'type',   type: 'INTEGER', mapping: {'file' => 1, 'dir' => 2}},
      {key: :name,   name: 'name',   type: 'TEXT'},
      {key: :size,   name: 'size',   type: 'INTEGER'},
      {key: :mode,   name: 'mode',   type: 'TEXT'}, # even though it's an integer, we usually think of this as a string, since each character maps to user/group/other
      {key: :mtime,  name: 'mtime',  type: 'INTEGER'},
      {key: :owner,  name: 'own',    type: 'TEXT'},
      {key: :group,  name: 'grp',    type: 'TEXT'},
      {key: :sha256, name: 'sha256', type: 'TEXT'},
      {key: :path,   name: 'path',   type: 'TEXT'},
    ]

    last_dir = nil

    # read the index file
    IndexFile::Reader.new(input_file(:index_path)) do |index_file|
      begin
        # create the database file
        db = SQLite3::Database.new output_file(:db_path)

        create_files_table(db)

        record_id = 1

        while not index_file.eof? do
          object = index_file.read_object

          # track last 'dir' entry
          if 'dir' == object[:type]
            last_dir = object
          end

          # write to CSV file
          if object_types_to_export.include? object[:type]
            # set a sequential record id
            object[:id] = record_id
            record_id += 1

            # the path comes from the more recent 'dir' entry
            object[:path] = last_dir[:path]

            values_string = db_columns.map do |db_column|
              key = db_column[:key]
              value = object[key]
              if mapping = db_column[:mapping]
                value = mapping[value]
              end
              case db_column[:type]
              when 'INTEGER'
                "#{value}"  # integers are not quoted
              when 'TEXT'
                escaped_value = value.gsub("'", "''") # single-quotes must be doubled in order to be properly understood by SQLite3
                "'#{escaped_value}'" # strings ARE quoted
              else
                raise "Column type '#{db_column[:type]}' is not handled, yet..."
              end
            end.join(",")

            sql = "INSERT INTO files VALUES(#{values_string})"
            # puts "sql: #{sql}"
            db.execute sql
          end
          
        end # while
      
      rescue SQLite3::Exception => e 
        puts "Exception occured"
        puts e

      ensure
        db.close if db

      end
    end # IndexFile::Reader
  end

  # merge two sqlite3 databases and output a single sqlite3 database
  # uses that largest database as the base, then adds only new records from the smaller db to the larger one
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

    #
    # merge small db_in to db_out, by reading all records and adding them to db_out (unless the record already exist)
    #

    db_columns_in_order = %w[ id type name size mode mtime own grp sha256 path ] # NOTE 'own' (not owner) and 'grp' (not group)
    db_columns_info = {
      'id'     => {type: :integer}, # 'id' will be handled in special ways (omitted in SELECT, rewritten in INSERT)
      'type'   => {type: :integer},
      'name'   => {type: :text},
      'size'   => {type: :integer},
      'mode'   => {type: :text},
      'mtime'  => {type: :integer},
      'own'    => {type: :text},
      'grp'    => {type: :text},
      'sha256' => {type: :text},
      'path'   => {type: :text},
    }

    db_in = nil
    db_out = nil

    begin
      # open db_in_small so we can read from it
      db_in = SQLite3::Database.open(db_in_small_path)
      db_in.results_as_hash = true

      # open db_out so we can write to it
      db_out = SQLite3::Database.open(db_out_path)
      db_out.results_as_hash = true

      # see how many rows are in each database
      sql = "SELECT COUNT(*) FROM files" 
      db_in_count = count_files(db_in)
      db_out_count = count_files(db_out)
      puts "db_in has #{db_in_count} rows"
      puts "db_out has #{db_out_count} rows"

      # find the largest 'id' in db_out
      sql = "SELECT MAX(id) FROM files"
      db_out_id_max = db_out.execute(sql).first[0]
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

      db_out.execute "BEGIN" # start transactions, for better performance

      next_id = db_out_id_max + 1
      in_rows.each do |in_row|
        # see if the in_row already exists in db_out
        fields_match = db_columns_in_order.map do |field|
          value = in_row[field]
          db_column = db_columns_info[field]

          case db_column[:type]
          when :integer
            if 'id' == field
              nil # skip the 'id' field when searching for existing record in db_out
            else
              "#{field}=#{value}"  # integers are not quoted
            end
          when :text
            escaped_value = value.gsub("'", "''") # single-quotes must be doubled in order to be properly understood by SQLite3
            "#{field}='#{escaped_value}'" # strings ARE quoted
          else
            raise "Column type '#{db_column[:type]}' is not handled, yet..."
          end
        end.reject{|v| v.nil?}

        conditions = fields_match.join(' AND ')
        sql = "SELECT COUNT(*) FROM files WHERE #{conditions}"
        # puts "sql: #{sql}"
        exist_count = db_out.execute(sql).first[0].to_i
      
        # if in_row does not exist in db_out, then instert it into db_out
        if exist_count == 0
          num_added += 1
          values_string = db_columns_in_order.map do |field|
            value = in_row[field]
            db_column = db_columns_info[field]

            case db_column[:type]
            when :integer
              if 'id' == field
                value = next_id   # give the record a new 'id' for it's place in db_out
                next_id += 1      # increment for next record
              end
              "#{value}"  # integers are not quoted
            when :text
              escaped_value = value.gsub("'", "''") # single-quotes must be doubled in order to be properly understood by SQLite3
              "'#{escaped_value}'" # strings ARE quoted
            else
              raise "Column type '#{db_column[:type]}' is not handled, yet..."
            end
          end.join(",")

          sql = "INSERT INTO files VALUES(#{values_string})"
          # puts "sql: #{sql}"
          result = db_out.execute sql
        else
          num_skipped += 1
        end
      end

      db_out.execute "COMMIT" # commit transactions, for better performance
      puts "num_added: #{num_added}"
      puts "num_skipped: #{num_skipped}"

    rescue SQLite3::Exception => e 
      puts "Exception occured"
      puts e

    ensure
      db_in.close if db_in
      db_out.close if db_out

    end
  end


  # merge two sqlite3 databases and output a single sqlite3 database
  # uses that largest database as the base, then adds only new records from the smaller db to the larger one
  # uses ruby hash to speed up the detection of duplicates
  def merge_sqlite3_hybrid(options = {})
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

    #
    # merge small db_in to db_out, by reading all records and adding them to db_out (unless the record already exist)
    #

    db_columns_in_order = %w[ id type name size mode mtime own grp sha256 path ] # NOTE 'own' (not owner) and 'grp' (not group)
    db_columns_info = {
      'id'     => {type: :integer}, # 'id' will be handled in special ways (omitted in SELECT, rewritten in INSERT)
      'type'   => {type: :integer},
      'name'   => {type: :text},
      'size'   => {type: :integer},
      'mode'   => {type: :text},
      'mtime'  => {type: :integer},
      'own'    => {type: :text},
      'grp'    => {type: :text},
      'sha256' => {type: :text},
      'path'   => {type: :text},
    }

    db_in = nil
    db_out = nil

    begin
      # open db_in_small so we can read from it
      db_in = SQLite3::Database.open(db_in_small_path)
      db_in.results_as_hash = true

      # read all records into a hash (this hash will be used for fast lookup later, to avoid adding duplicate entries)
      unique_files = {}
      import_files_into_hash(db_in, unique_files)

      # open db_out so we can write to it
      db_out = SQLite3::Database.open(db_out_path)
      db_out.results_as_hash = true

      # see how many rows are in each database
      sql = "SELECT COUNT(*) FROM files" 
      db_in_count = count_files(db_in)
      db_out_count = count_files(db_out)
      puts "db_in has #{db_in_count} rows"
      puts "db_out has #{db_out_count} rows"

      # find the largest 'id' in db_out
      sql = "SELECT MAX(id) FROM files"
      db_out_id_max = db_out.execute(sql).first[0]
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

      db_out.execute "BEGIN" # start transactions, for better performance

      next_id = db_out_id_max + 1
      in_rows.each do |in_row|
        # see if the in_row already exists in db_out
        key_string = calculate_key_string_for_files_row(in_row)

        # if in_row does not exist in db_out, then instert it into db_out
        unless unique_files.key? key_string
          unique_files[key_string] = create_hash_for_files_row(in_row)
          num_added += 1
          values_string = db_columns_in_order.map do |field|
            value = in_row[field]
            db_column = db_columns_info[field]

            case db_column[:type]
            when :integer
              if 'id' == field
                value = next_id   # give the record a new 'id' for it's place in db_out
                next_id += 1      # increment for next record
              end
              "#{value}"  # integers are not quoted
            when :text
              escaped_value = value.gsub("'", "''") # single-quotes must be doubled in order to be properly understood by SQLite3
              "'#{escaped_value}'" # strings ARE quoted
            else
              raise "Column type '#{db_column[:type]}' is not handled, yet..."
            end
          end.join(",")

          sql = "INSERT INTO files VALUES(#{values_string})"
          # puts "sql: #{sql}"
          result = db_out.execute sql
        else
          num_skipped += 1
        end
      end

      db_out.execute "COMMIT" # commit transactions, for better performance
      puts "num_added: #{num_added}"
      puts "num_skipped: #{num_skipped}"

    rescue SQLite3::Exception => e 
      puts "Exception occured"
      puts e

    ensure
      db_in.close if db_in
      db_out.close if db_out

    end
  end


  # merge two sqlite3 databases and output a single sqlite3 database
  # uses ruby hash to collect unique records, then writes out a brand new database from that hash
  def merge_sqlite3_fast(options = {})
    required_input_files :db_in1_path, :db_in2_path
    required_output_files :db_out_path

    db_in1_path = input_file(:db_in1_path)
    db_in2_path = input_file(:db_in2_path)
    db_out_path = output_file(:db_out_path)

    # build a hash of all records, indexed by a checksum of the uniqueness attributes
    uniqueness_attributes = %w[ type name size mode mtime own grp sha256 path ] # NOT id (just a sequence number)
    unique_files = {}

    #
    # read both files and add files to the unique_files hash
    #

    db_columns_in_order = %w[ id type name size mode mtime own grp sha256 path ] # NOTE 'own' (not owner) and 'grp' (not group)
    db_columns_info = {
      'id'     => {type: :integer}, # 'id' will be handled in special ways (omitted in SELECT, rewritten in INSERT)
      'type'   => {type: :integer},
      'name'   => {type: :text},
      'size'   => {type: :integer},
      'mode'   => {type: :text},
      'mtime'  => {type: :integer},
      'own'    => {type: :text},
      'grp'    => {type: :text},
      'sha256' => {type: :text},
      'path'   => {type: :text},
    }

    #
    # read both files and add files to the unique_files hash
    #

    [db_in1_path, db_in2_path].each do |db_in_path|
      begin
        # open db_in_path so we can read from it
        puts "opening db '#{db_in_path}'"
        db_in = SQLite3::Database.open(db_in_path)
        db_in.results_as_hash = true

        # get all the records from db_in
        sql = "SELECT * FROM files" 
        # puts "sql: #{sql}"
        in_rows = db_in.execute sql
        puts "file records: #{in_rows.size}"

        num_added = 0
        num_skipped = 0

        # process each row, insert into unique_files hash unless the hash already contains that entry
        in_rows.each do |in_row|
          key_string = uniqueness_attributes.map{|attr| in_row[attr]}.join('+')
          key = StringHash.sha256(key_string)
          unless unique_files.key? key
            # convert array to attr=>value hash
            file_attributes = Hash[* uniqueness_attributes.map{|attr| [attr, in_row[attr]]}.flatten]
            unique_files[key] = file_attributes
            num_added += 1
          else
            num_skipped += 1
          end
        end
        puts "files added: #{num_added}"
        puts "files skipped: #{num_skipped}"

      rescue SQLite3::Exception => e 
        puts "Exception occured"
        puts e

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

      create_files_table(db_out)

      # start a transaction, so we can insert all rows in one large operations, which is MUCH faster
      result = db_out.execute "BEGIN"

      puts "file records to write: #{unique_files.count}"
      next_id = 1
      unique_files.each do |key, file_attributes|
        values_string = db_columns_in_order.map do |db_column|
          file_attributes['id'] = next_id
          next_id += 1

          value = file_attributes[db_column]
          db_column = db_columns_info[db_column]

          case db_column[:type]
          when :integer
            "#{value}"  # integers are not quoted
          when :text
            escaped_value = value.gsub("'", "''") # single-quotes must be doubled in order to be properly understood by SQLite3
            "'#{escaped_value}'" # strings ARE quoted
          else
            raise "Column type '#{db_column[:type]}' is not handled, yet..."
          end
        end.join(",")

        sql = "INSERT INTO files VALUES(#{values_string})"
        # puts "sql: #{sql}"
        result = db_out.execute sql
      end
      puts "next_id after last write: #{next_id}"
        
    rescue SQLite3::Exception => e 
      puts "Exception occured"
      puts e.message
      puts e.backtrace

    ensure
      # end the transaction
      result = db_out.execute "COMMIT"

      db_out.close if db_out

    end
  end # def merge_sqlite3_fast

end
