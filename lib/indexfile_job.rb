require File.expand_path('logging', File.dirname(__FILE__))
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

class IndexFileJob < Job
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
  # sqlite3 job methods
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
      logger.info "files_count: #{files_count}"
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
        logger.debug "record_id after last write: #{record_id}"
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
      logger.debug "db_in1 has #{db_in1_count} rows"

      # get all the records from db_in1
      sql = "SELECT * FROM files" 
      in1_rows = db_in1.execute sql
      logger.debug "rows to diff: #{in1_rows.count}"

      # open db_in2 so we can read from it
      DbSqlite3.open_database(db_in2_path) do |db_in2|

        # see how many rows are in the 'files' table
        db_in2_count = db_in2.count_rows(:files)
        logger.debug "db_in2 has #{db_in2_count} rows"

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
          logger.debug "num_unique: #{num_unique}"
          logger.debug "num_common: #{num_common}"
        end # db_out
      end # db_in2
    end # db_in1

  end


  # merge two sqlite3 databases and output a single sqlite3 database
  # uses that largest database as the startign point, then adds only new records from the smaller db to the larger one
  #
  # Performance profile:
  # - Fast, because SQL SELECT is fast when there is an INDEX on the 'files' table
  # - Uses very little memory, because records are processed and then forgotten, it's the DB's worker to find duplicates for us (and that's why it's slow)
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
      logger.debug "db_in has #{db_in_count} rows"

      # open db_in_large so we can read from it (to check if a record already exist or not)
      DbSqlite3.open_database(db_in_large_path) do |db_in_large|

        # open db_out so we can write to it
        DbSqlite3.open_database(db_out_path) do |db_out|

          # see how many rows are in the 'files' table
          db_out_count = db_out.count_rows(:files)
          logger.debug "db_out has #{db_out_count} rows"

          # find the largest 'id' in db_out
          db_out_id_max = db_out.max_id(:files)
          logger.debug "db_out MAX(id): #{db_out_id_max}"

          # get all the records from db_in
          # columns_string = columns_to_merge.join(',')
          # sql = "SELECT #{columns_string} FROM files" 
          sql = "SELECT * FROM files" 
          # logger.debug "sql: #{sql}"
          in_rows = db_in.execute sql
          logger.debug "rows to merge: #{in_rows.count}"
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
          logger.info "num_added: #{num_added}"
          logger.info "num_skipped: #{num_skipped}"
        end # db_out
      end # db_in_large
    end # db_in

  end


end # class IndexFileJob
