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

  # exports a CSV file from a binary index file
  require 'sqlite3'
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

        # create table
        columns_string = db_columns.map{|col| "#{col[:name]} #{col[:type]} #{col[:extra]}"}.join(", ")
        sql = "CREATE TABLE IF NOT EXISTS files(#{columns_string})" 
        puts "sql: #{sql}"
        db.execute sql

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
                "'#{value}'" # strings ARE quoted
              else
                raise "Column type '#{db_column[:type]}' is not handled, yet..."
              end
            end.join(",")

            sql = "INSERT INTO files VALUES(#{values_string})"
            puts "sql: #{sql}"
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

end
