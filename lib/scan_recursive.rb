require File.join(File.dirname(__FILE__), 'hasher')
require File.join(File.dirname(__FILE__), 'pathinfo')
require File.join(File.dirname(__FILE__), 'indexfile')
require File.join(File.dirname(__FILE__), 'pipeline')

require 'pathname'


# scan a directory and all it's sub-directories (recursively) and process all entries along teh way
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
def scan_recursive(index_file, scan_info, path, &block)
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

  yield(path, dir_info_initial) if block_given?
  index_file.write_object(dir_info_initial)

  symlinks = []
  dirs = []
  files = []
  content_size = 0
  content_hashes = []
  meta_hashes = []

  #
  # process each entry in the current directory, capturing relevant metadata for each entry
  #
  Dir[File.join(base_path, '{*,.*}')].sort.each do |full_path|  # sort is important for deterministic content hash
                                                                # for the entire dir
    puts "scan: #{full_path}"
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

        yield(path, symlink_info) if block_given?
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
          # file_info[:md5] = FileHash.md5(full_path)
          file_info[:sha256] = FileHash.sha256(full_path)

          hasher = Hasher.new(scan_info[:file_hash_template], file_info)
          file_info[:hash_src] = hasher.source
          file_info[:hash] = hasher.hash
          # accumulate
          content_hashes << file_info[:sha256]
          meta_hashes << file_info[:hash]
        end

        yield(path, file_info) if block_given?
        index_file.write_object(file_info)
      else
        unknown_info = {
          :type => :unknown,
          :name => name
        }
        yield(path, unknown_info) if block_given?
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
      sub_dir_info = scan_recursive(index_file, scan_info, File.join(path, dir), &block)

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

  yield(path, dir_info_final) if block_given?
  index_file.write_object(dir_info_final)

  # return the summary of all the entries in the folder (including recursive summary)
  return dir_info_final
end
