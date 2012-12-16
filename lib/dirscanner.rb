require File.join(File.dirname(__FILE__), 'hasher')
require File.join(File.dirname(__FILE__), 'pathinfo')
require File.join(File.dirname(__FILE__), 'indexfile')
require File.join(File.dirname(__FILE__), 'pipeline')

require 'pathname'
require 'json'
require 'bindata'
require 'socket'

class DirScanner < Worker
  attr_accessor :inputs, :outputs

  # perform a directory scan, by inspecting all files, symlinks and folders (recursively)
  def scan()
    required_inputs :scan_root
    required_outputs :scan_index

    timestamp = Time.now.to_i unless @timestamp
    
    # index file will be inside the scan_root foler, unless otherwise specified
    # output(:scan_index) ||= File.join(real_scan_root, ".dirscan_#{timestamp}")

    # create scan object, contains meta-data for the entire scan
    scan_info = {
      :type => :dirscan,
      :host_name => Socket.gethostname,
      :scan_root => input(:scan_root),
      :scan_root_real => Pathname.new(input(:scan_root)).realpath,  # turn scan_root into the canonical form, making it absolute, with no symlinks
      :timestamp => timestamp,
      :index_path => output(:scan_index),
      :verbose => input(:verbose, :default => false),
    }

    # templates specify how to create the hash source strings for various dir entry types
    if input(:quick_scan, :default => false)
      # quick scan does not need hash templates
      scan_info[:quick] = true
    else
      scan_info.merge!({
        :symlink_hash_template  => 'name+mode+owner+group+mtime+size'.freeze, # size of symlink itself, not the target
        :file_hash_template     => 'name+mode+owner+group+mtime+size+sha256'.freeze,  # size and content hash
        :dir_hash_template      => 'name+mode+owner+group+mtime+content_size+content_hash+meta_hash'.freeze,  # size/hash of dir's content
      })
    end

    # create the index file, and perform the scan
    IndexFile::Writer.new(output(:scan_index)) do |index_file|
      # write dirscan meta-data
      index_file.write_object(scan_info)

      # scan recursively
      @scan_result = scan_recursive(index_file, scan_info, input(:scan_root))
    end

    return @scan_result
  end

  def unpack()
    required_inputs :scan_index
    required_outputs :scan_unpack

    # read the index file
    IndexFile::Reader.new(input(:scan_index)) do |index_file|
      # write the text file
      File.open(output(:scan_unpack), 'w') do |unpack_file|
        while not index_file.eof? do
          object = index_file.read_object
          unpack_file.write JSON.pretty_generate(object) + "\n"
        end
      end
    end
  end


  def extract()
    required_inputs :scan_index

    result = {
      :dirscan => nil,
      :dirs => {},
    }
    object_count = 0

    # read the index file
    IndexFile::Reader.new(input(:scan_index)) do |index_file|
      # state objects, updated during parsing
      dir = nil

      # read from index file until we reach the end
      while not index_file.eof? do
        object = index_file.read_object
        # puts "object: #{object.inspect}"
        object_count += 1
        
        case object[:type].to_sym
        when :dirscan
          result[:dirscan] = object
        when :dir
          dir = object
          # store in result, or merge it if an initial record already exist
          path = dir[:path]
          if result[:dirs][path]
            result[:dirs][path].merge! dir
          else
            result[:dirs][path] = dir
            # prep dir to hold child entries
            dir[:entries] ||= {}
          end

          # set current dir
          dir = result[:dirs][path]
        when :symlink
          symlink = object
          # store in result, inside current dir
          name = symlink[:name]
          dir[:entries][name] = symlink
        when :file
          file = object
          # store in result, inside current dir
          name = file[:name]
          dir[:entries][name] = file
        end
      end
    end

    @extract_result = result
    return @extract_result
  end


  def verify()
    required_inputs :scan_index

    object_count = 0
    issues_count = 0

    # read the index file
    IndexFile::Reader.new(input(:scan_index)) do |index_file|
      # state objects, updated during parsing
      dirscan = {}
      dir = {}

      # read from index file until we reach the end
      while not index_file.eof? do
        object = index_file.read_object
        object_count += 1
        
        case object[:type]
        when :dirscan
          dirscan = object
        when :dir
          dir = object
        when :symlink
          symlink = object
        when :file
          file = object
          full_path = File.join dir[:path], file[:name]
          if File.exist?(full_path)
            size = File.size(full_path)
            identical = true
            identical = false if size != file[:size]
            identical = false if FileHash.md5(full_path) != file[:md5]
            identical = false if FileHash.sha256(full_path) != file[:sha256]
            issues_count += 1 unless identical
          else
            issues_count += 1
          end
        end
      end
    end

    @verify_result = {
      :issues_count => issues_count
    }
    return @verify_result
  end


  def analyze()
    required_inputs :scan_index
    required_outputs :analysis

    # read the index file
    IndexFile::Reader.new(input(:scan_index)) do |index_file|
      # analysis object
      file_sizes = {}
      analysis = {
        file_sizes: file_sizes
      }

      # iterate over all the entries
      while not index_file.eof? do
        object = index_file.read_object

        case object[:type].to_sym
        when :dirscan
        when :dir
        when :symlink
        when :file
          file = object
          size = file[:size]
          # increment file size counter
          file_sizes[size] = (file_sizes[size] || 0) + 1
        end
      end

      # write the text file
      File.open(output(:analysis), 'w') do |text_file|
        text_file.write JSON.pretty_generate(analysis) + "\n"
      end
  
      return analysis
    end
  end

  def analysis_report
    required_inputs :analysis
    required_outputs :analysis_report

    analysis = File.open(input(:analysis)){|f| JSON.load(f)}
    file_sizes = analysis['file_sizes']
    sorted_file_sizes = file_sizes.keys.map{|key| key.to_i}.sort
    sizes_with_counts = sorted_file_sizes.map{|size| [size, file_sizes["#{size}"]]}
    sorted_by_count = sizes_with_counts.sort{|a,b| b[1] <=> a[1]}
    report = {
      # sorted_file_sizes: sizes_with_counts,
      sorted_by_count: sorted_by_count,
    }

    # write the text file
    File.open(output(:analysis_report), 'w') do |text_file|
      text_file.write JSON.pretty_generate(report) + "\n"
    end

    return report
  end

  def iddupe()
    required_inputs :scan_index, :analysis
    required_outputs :iddupe

    # load up the analysis
    analysis = File.open(input(:analysis)){|f| JSON.load(f)}
    file_sizes = analysis['file_sizes']

    # create a list of file sizes that should be inspected more carefully
    collection_by_file_size = {}
    file_sizes.each do |size, num|
      size_i = size.to_i
      if size_i > 0 && num > 1
        collection_by_file_size[size_i] = {}  # this hash will collect SHA256 checksums for all files of this size
      end
    end


    # read the index file
    IndexFile::Reader.new(input(:scan_index)) do |index_file|
      # state objects, updated during parsing
      dirscan = {}
      dir = {}

      # iterate over all the entries
      while not index_file.eof? do
        object = index_file.read_object

        case object[:type].to_sym

        when :dirscan
          dirscan = object
        when :dir
          dir = object
        when :file
          file = object
          size_i = file[:size].to_i
          if collection = collection_by_file_size[size_i]
            # puts "dirscan[:scan_root] = #{dirscan[:scan_root]}"
            # puts "dir[:path] = #{dir[:path]}"
            # puts "file[:name] = #{file[:name]}"
            full_path = File.join(dir[:path], file[:name])
            sha256 = FileHash.sha256(full_path)
            if sha256
              collection[sha256] ||= []
              collection[sha256] << full_path
            end
          end
        end
      end

      # remove arrays with only a sigle entry
      collection_by_file_size.each do |file_size, collection|
        collection.keys.each do |sha256|
          if collection[sha256].size == 1
            collection.delete(sha256)
          end
        end
      end
      # remove empty collections
      collection_by_file_size.keys.each do |file_size|
        if collection_by_file_size[file_size].empty?
          collection_by_file_size.delete(file_size)
        end
      end

      result = {
        :collection_by_file_size => collection_by_file_size
      }

      # write the text file
      File.open(output(:iddupe), 'w') do |text_file|
        text_file.write JSON.pretty_generate(result) + "\n"
      end
  
      return result
    end
  end

  def iddupe_report
    required_inputs :iddupe
    required_outputs :iddupe_report

    iddupe = File.open(input(:iddupe)){|f| JSON.load(f)}
    collection_by_file_size = iddupe['collection_by_file_size']
    sorted_file_sizes = collection_by_file_size.keys.map{|key| key.to_i}.sort.reverse # largest files first
    sizes_with_dupes = sorted_file_sizes.map do |size|
      dupes = collection_by_file_size["#{size}"]
      redundant_size = 0
      dupes.each do |sha256, paths|
        redundant_size += size * (paths.size-1)
      end
      [size, redundant_size, dupes]
    end

    report = {
      # sorted_file_sizes: sizes_with_counts,
      sizes_with_dupes: sizes_with_dupes,
    }

    # write the text file
    File.open(output(:iddupe_report), 'w') do |text_file|
      text_file.write JSON.pretty_generate(report) + "\n"
    end

    return report
  end


  # scan a directory and all it's sub-directories (recursively)
  #
  # Consider this example directory tree:
  #   dir-a
  #     file-1
  #     symlink-x
  #     file-2
  #     dir-b
  #       file 3
  #
  # The records would be written in this order (dir records are written twice, first initial, then final):
  #   dir-a (initial)
  #   file-1
  #   symlink-x
  #   file-2
  #   dir-b (initial)
  #   file-3
  #   dir-b (final)
  #   dir-a (final)
  #
  def scan_recursive(index_file, scan_info, path)
    base_path = Pathname.new(path)

    pathinfo = PathInfo.new(path)

    # write initial dir record
    dir_info_initial = {
      :type => :dir,
      :path => path,
      :name => File.basename(path),
      :mode => pathinfo.mode,
      # :ctime => pathinfo.create_time,
      :mtime => pathinfo.modify_time,
      :owner => pathinfo.owner,
      :group => pathinfo.group,
    }

    index_file.write_object(dir_info_initial)

    symlinks = []
    dirs = []
    files = []
    content_size = 0
    content_hashes = []
    meta_hashes = []

    Dir[File.join(base_path, '{*,.*}')].each do |full_path|
      pathinfo = PathInfo.new(full_path)
      name = Pathname.new(full_path).relative_path_from(base_path).to_s # .to_s converts from Pathname to actual string
      case name
      when '.'  # current dir
      when '..' # parent dir
      else

        if File.symlink?(full_path)
          symlinks << name

          # get the sie of the symlink itself (not the size of what it's pointing at)
          # size = File.lstat(full_path).size
          # DO NOT NEED THIS since a symlink does not count as real content (a folder is not real content either, only files are)

          symlink_info = {
            :type => :symlink,
            :name => name,
            :link_path => File.readlink(full_path),
            :mode => pathinfo.mode,
            # :ctime => pathinfo.create_time,
            :mtime => pathinfo.modify_time,
            :owner => pathinfo.owner,
            :group => pathinfo.group,
          }
          unless scan_info[:quick]
            hasher = Hasher.new(scan_info[:symlink_hash_template], symlink_info)
            symlink_info[:hash_src] = hasher.source
            symlink_info[:hash] = hasher.hash

            # accumulate
            meta_hashes << symlink_info[:hash]
            # content_hash does not exist for symlinks
          end
  
          index_file.write_object(symlink_info)
        elsif Dir.exist?(full_path)
          # recurse into sub dirs after completing this dir scan, tally things up at the end...
          dirs << name
        elsif File.exist?(full_path)
          files << name

          size = File.size(full_path)
          content_size += size

          file_info = {
            :type => :file,
            :name => name,
            :size => size,
            :mode => pathinfo.mode,
            # :ctime => pathinfo.create_time,
            :mtime => pathinfo.modify_time,
            :owner => pathinfo.owner,
            :group => pathinfo.group,
            
            
          }
          unless scan_info[:quick]
            file_info[:md5] = FileHash.md5(full_path)
            file_info[:sha256] = FileHash.sha256(full_path)

            hasher = Hasher.new(scan_info[:file_hash_template], file_info)
            file_info[:hash_src] = hasher.source
            file_info[:hash] = hasher.hash
            # accumulate
            content_hashes << file_info[:sha256]
            meta_hashes << file_info[:hash]
          end

          index_file.write_object(file_info)
        else
          unknown_info = {
            :type => :unknown,
            :name => name
          }
          index_file.write_object(unknown_info)
        end
      end
    end

    # recursive = stats for this dir + stats for all subdirs
    #
    # the properties inside 'recursive' are kept separate from the properties for just the current dir,
    # so that we can report the simple 'this dir only' and also the full recursive status.
    recursive = {
      :content_size => content_size,
      :symlink_count => symlinks.count,
      :dir_count => dirs.count,
      :file_count => files.count,
      :max_depth => 0, # 0 means empty dir, 1 means the dir only contains files or symlinks, > 1 indicates subdirs
    }
    unless scan_info[:quick]
      recursive.merge!({
        :content_hashes => content_hashes.dup,    # clone array, so 'recursive' can keep adding to it's copy
        :meta_hashes => meta_hashes.dup,          # clone array, so 'recursive' can keep adding to it's copy
      })
    end

    if dirs.count > 0
      dirs.each do |dir|
        puts "Scanning subdir #{dir} of #{path}" if scan_info[:verbose]
        sub_dir_info = scan_recursive(index_file, scan_info, File.join(path, dir))

        #
        # accumulation (for current level onlY)
        #

        # accumulate folder meta-data
        meta_hashes << sub_dir_info[:hash]
        # content_hash does not make sense for a dir at this level (since we don't recurse, it's effecively empty)

        #
        # recursive accumulation
        #

        # accumulate sizes and counts
        recursive[:content_size] += sub_dir_info[:recursive][:content_size]
        recursive[:symlink_count] += sub_dir_info[:recursive][:symlink_count]
        recursive[:dir_count] += sub_dir_info[:recursive][:dir_count]
        recursive[:file_count] += sub_dir_info[:recursive][:file_count]
        recursive[:max_depth] = [recursive[:max_depth], 1 + sub_dir_info[:recursive][:max_depth]].max
        
        # accumulate hash source strings
        unless scan_info[:quick]
          recursive[:content_hashes] << sub_dir_info[:recursive][:content_hash]
          recursive[:meta_hashes] << sub_dir_info[:recursive][:meta_hash]
        end
      end
    else
      max_depth = 1 if files.count > 0 || symlinks.count > 0
    end

    # finalize the hashes
    content_hash_src = content_hashes.join(HASH_SRC_JOIN)
    meta_hash_src = meta_hashes.join(HASH_SRC_JOIN)
    content_hash = StringHash.md5(content_hash_src)
    meta_hash = StringHash.md5(meta_hash_src)

    unless scan_info[:quick]
      # finalize the recursive hashes (from their source strings)
      recursive[:content_hash_src] = recursive[:content_hashes].join(HASH_SRC_JOIN)
      recursive[:meta_hash_src] = recursive[:meta_hashes].join(HASH_SRC_JOIN)
      recursive[:content_hash] = StringHash.md5(recursive[:content_hash_src])
      recursive[:meta_hash] = StringHash.md5(recursive[:meta_hash_src])
    end

    # write final dir record
    dir_info_final = dir_info_initial.merge({
      :symlink_count => symlinks.count,
      :dir_count => dirs.count,
      :file_count => files.count,
      :content_size => content_size,
      # recursive summary
      :recursive => recursive,
    })
    unless scan_info[:quick]
      dir_info_final.merge!({
        # hashes
        :content_hash_src => content_hash_src,
        :meta_hash_src => meta_hash_src,
        :content_hash => content_hash,
        :meta_hash => meta_hash,
      })
      hasher = Hasher.new(scan_info[:dir_hash_template], dir_info_final)
      dir_info_final[:hash_src] = hasher.source
      dir_info_final[:hash] = hasher.hash
    end

    index_file.write_object(dir_info_final)

    # return the summary of all the entries in the folder (including recursive summary)
    return dir_info_final
  end

end

