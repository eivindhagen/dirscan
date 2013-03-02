require File.join(File.dirname(__FILE__), 'hasher')
require File.join(File.dirname(__FILE__), 'hostinfo')
require File.join(File.dirname(__FILE__), 'pathinfo')
require File.join(File.dirname(__FILE__), 'indexfile')
require File.join(File.dirname(__FILE__), 'pipeline')
require File.join(File.dirname(__FILE__), 'scan_recursive')
require File.join(File.dirname(__FILE__), 'db_sqlite3')

require 'pathname'
require 'json'
require 'bindata'
require 'fileutils'

class FilePileJob < Job
  attr_accessor :inputs, :outputs

  # store all files located in the input folder (recursive) to the FilePile
  def store(options = {})
    required_input_files :scan_root
    required_output_files :filepile_root

    timestamp = Time.now.to_i

    scan_root = input_file(:scan_root)
    filepile_root = output_file(:filepile_root)
    scan_index = output_file(:scan_index)

    filepile = FilePileDir.new(filepile_root)
    filedata_path = filepile.filedata

    puts "store()"
    puts " scan_root: " + scan_root
    puts " filepile_root: " + filepile_root
    puts " scan_index: " + scan_index

    # create scan object, contains meta-data for the entire scan
    scan_info = {
      :type => :store,
      :host_name => HostInfo.name,
      :scan_root => input_file(:scan_root),
      :scan_root_real => Pathname.new(input_file(:scan_root)).realpath,  # turn scan_root into the canonical form, making it absolute, with no symlinks
      :timestamp => timestamp,
      :index_path => scan_index,
      :verbose => input_value(:verbose, :default => false),
    }

    # templates specify how to create the hash source strings for various dir entry types
    scan_info.merge!({
      :symlink_hash_template  => 'name+mode+owner+group+mtime+link_path'.freeze, # size of symlink itself, not the target
      :file_hash_template     => 'name+mode+owner+group+mtime+size+sha256'.freeze,  # size and content hash
      :dir_hash_template      => 'name+mode+owner+group+mtime+content_size+content_hash+meta_hash'.freeze,  # size/hash of dir's content
    })

    # create the index file, and store all files in the file pile
    IndexFile::Writer.new(scan_index) do |index_file|
      # write dirscan meta-data
      index_file.write_object(scan_info)

      # scan recursively
      @scan_result = scan_recursive(index_file, scan_info, scan_root) do |path, info|
        # puts "block: info[:type]=#{info[:type]}"
        case info[:type]
        when :file
          # copy this file to the FilePile area, using the sha256 checksum as the filename
          src_path = File.join(path, info[:name])
          dst_name = info[:sha256]
          dst_dir = File.join(filedata_path, dst_name[0..1], dst_name[2..3], dst_name[4..5])
          dst_path = File.join(dst_dir, dst_name)
          unless File.exist?(dst_path)
            FileUtils.mkdir_p(dst_dir) unless Dir.exist?(dst_dir)
            FileUtils.copy_file(src_path, dst_path)
          end
        end
      end
    end

    return @scan_result
  end

  # Store all files located in the input folder (recursive) to the FilePile, but ignore any file that is already in the FilePile.
  # The FilePile's existing metadata DB is used to detect files that already exist in the FilePile
  #
  # For performance reasons, a file is cone
  def store_update(options = {})
    required_input_files :scan_root
    required_output_files :filepile_root

    timestamp = Time.now.to_i

    scan_root = input_file(:scan_root)
    filepile_root = output_file(:filepile_root)
    scan_index = output_file(:scan_index)

    puts "store_update()"
    puts " scan_root: " + scan_root
    puts " filepile_root: " + filepile_root
    puts " scan_index: " + scan_index

    filepile = FilePileDir.new(filepile_root)
    filedata_path = filepile.filedata
    metadata_db_path = filepile.db_path

    DbSqlite3.open_or_create_database(metadata_db_path) do |db|

      # create scan object, contains meta-for data the entire scan
      scan_info = {
        :type => :store_update,
        :host_name => HostInfo.name,
        :scan_root => input_file(:scan_root),
        :scan_root_real => Pathname.new(input_file(:scan_root)).realpath,  # turn scan_root into the canonical form, making it absolute, with no symlinks
        :timestamp => timestamp,
        :index_path => scan_index,
        :verbose => input_value(:verbose, :default => false),
      }

      # templates specify how to create the hash source strings for various dir entry types
      scan_info.merge!({
        :symlink_hash_template  => 'name+mode+owner+group+mtime+link_path'.freeze, # size of symlink itself, not the target
        :file_hash_template     => 'name+mode+owner+group+mtime+size+sha256'.freeze,  # size and content hash
        :dir_hash_template      => 'name+mode+owner+group+mtime+content_size+content_hash+meta_hash'.freeze,  # size/hash of dir's content
      })

      # create the index file, and store all files in the file pile
      IndexFile::Writer.new(scan_index) do |index_file|
        # write dirscan meta-data
        index_file.write_object(scan_info)

        # scan recursively
        @scan_result = scan_recursive(index_file, scan_info, scan_root, db) do |path, info|
          # puts "block: info[:type]=#{info[:type]}"
          case info[:type]
          when :dir
            # do nothing

          when :file
            # skip copying if the file already exist in FilePile
            if info[:sha256] # if :sha256 value exist, then it's because we got this from the FilePile database # [:exists_in_filepile]
              # FOUND in db, so don't worry about copying file since it's already there
              # puts "#{info[:name]} : FOUND in DB"
            else
              # puts "#{info[:name]} : NOT in DB"
              # copy this file to the FilePile area, using the sha256 checksum as the filename
              src_path = File.join(path, info[:name])
              dst_name = info[:sha256]
              dst_dir = File.join(filedata_path, dst_name[0..1], dst_name[2..3], dst_name[4..5])
              dst_path = File.join(dst_dir, dst_name)
              unless File.exist?(dst_path) # don't copy of the file exist already (although this should have been caught by asking the database, above)
                FileUtils.mkdir_p(dst_dir) unless Dir.exist?(dst_dir)
                FileUtils.copy_file(src_path, dst_path)
              end
            end
          end
        end

        return @scan_result

      end # index_file
    end # db
  end

end # class FilePileJob
