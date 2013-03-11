require File.expand_path('logging', File.dirname(__FILE__))
require File.join(File.dirname(__FILE__), 'hasher')
require File.join(File.dirname(__FILE__), 'hostinfo')
require File.join(File.dirname(__FILE__), 'pathinfo')
require File.join(File.dirname(__FILE__), 'indexfile')
require File.join(File.dirname(__FILE__), 'pipeline')
require File.join(File.dirname(__FILE__), 'deep_scanner')
require File.join(File.dirname(__FILE__), 'db_sqlite3')
require File.join(File.dirname(__FILE__), 'file_info_db')

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

    logger.debug "store()"
    logger.debug " scan_root: " + scan_root
    logger.debug " filepile_root: " + filepile_root
    logger.debug " scan_index: " + scan_index

    # create scan object, contains meta-data for the entire scan
    scan_info = {
      :type => :store,
      :host_name => HostInfo.name,
      :scan_root => input_file(:scan_root),
      :scan_root_real => Pathname.new(input_file(:scan_root)).realpath,  # turn scan_root into the canonical form, making it absolute, with no symlinks
      :timestamp => timestamp,
      :index_path => scan_index,
      :verbose => input_value(:verbose, :default => false),
      :ignore_filters => [
        "/AppData/Local",
        "/AppData/Roaming",
      ]
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
      @scan_result = DeepScanner.scan_recursive(index_file, scan_info, scan_root) do |path, info|
        # logger.debug "block: info[:type]=#{info[:type]}"
        case info[:type]
        when :file
          # copy this file to the FilePile area, using the sha256 checksum as the filename
          src_path = File.join(path, info[:name])
          dst_name = info[:sha256]
          if dst_name
            dst_dir = File.join(filedata_path, dst_name[0..1], dst_name[2..3], dst_name[4..5])
            dst_path = File.join(dst_dir, dst_name)
            unless File.exist?(dst_path)
              FileUtils.mkdir_p(dst_dir) unless Dir.exist?(dst_dir)
              FileUtils.copy_file(src_path, dst_path)
            end
          else
            puts "ERROR: No sha256 value for path '#{src_path}'"
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

    logger.info "store_update()"
    logger.info " scan_root: " + scan_root
    logger.info " filepile_root: " + filepile_root
    logger.info " scan_index: " + scan_index

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
        @scan_result = DeepScanner.scan_recursive(index_file, scan_info, scan_root, db) do |path, info|
          # logger.debug "block: info[:type]=#{info[:type]}"
          case info[:type]
          when :dir
            # do nothing

          when :file
            # skip copying if the file already exist in FilePile
            if info[:sha256] # if :sha256 value exist, then it's because we got this from the FilePile database # [:exists_in_filepile]
              # FOUND in db, so don't worry about copying file since it's already there
              # logger.debug "#{info[:name]} : FOUND in DB"
            else
              # logger.debug "#{info[:name]} : NOT in DB"
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

  # scan a directory and add info for each file to the DB
  #
  #
  def scan_to_db(options = {})
    required_input_files :scan_root
    required_output_files :db_path

    scan_root = input_file(:scan_root)
    db_path = output_file(:db_path)

    logger.info "scan_to_db()"
    logger.info " scan_root: " + scan_root
    logger.info " db_path: " + db_path

    db = FileInfoDb.new(db_path)

    # create scan object, contains meta-for data the entire scan
    timestamp = Time.now.to_i
    scan_info = {
      :type => :store_update,
      :host_name => HostInfo.name,
      :scan_root => input_file(:scan_root),
      :scan_root_real => Pathname.new(input_file(:scan_root)).realpath,  # turn scan_root into the canonical form, making it absolute, with no symlinks
      :timestamp => timestamp,
      :db_path => db_path,
      :verbose => input_value(:verbose, :default => false),
    }

    # templates specify how to create the hash source strings for various dir entry types
    scan_info.merge!({
      :symlink_hash_template  => 'name+mode+owner+group+mtime+link_path'.freeze, # size of symlink itself, not the target
      :file_hash_template     => 'name+mode+owner+group+mtime+size+sha256'.freeze,  # size and content hash
      :dir_hash_template      => 'name+mode+owner+group+mtime+content_size+content_hash+meta_hash'.freeze,  # size/hash of dir's content
    })

    # log scan_info for posterity
    # TODO: store this in the DB as well, in another table
    logger.info scan_info.to_yaml

    # scan recursively (wihtout a DB to check for existing sha256, since we want this scan to be as simple as possible)
    @scan_result = DeepScanner.scan_recursive_simple(scan_info, scan_root) do |path, info|
      # logger.debug "block: info[:type]=#{info[:type]}"
      case info[:type]
      when :dir
        # do nothing

      when :file
        # file_info = FileInfoDb::FileInfo.create({
        #   type: 1,  # 1 = File
        #   name: info[:name],
        #   size: info[:size,
        #   mode: info[:mode],
        #   mtime: info[:mtime],
        #   own: info[:owner],
        #   grp: info[:group],
        #   sha256: nil,
        #   path: info[:path],
        # })
      end
    end

    return @scan_result
  end

end # class FilePileJob
