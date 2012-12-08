# dirscan.rb

require 'socket'
require 'pathname'
require 'json'
require 'yaml'
require 'BinData'
require 'etc'

require File.join(File.dirname(__FILE__), 'lib', 'hasher')



def file_mode(file)
	begin
		mode_integer = File.lstat(file).mode & 0777
		mode_octal_string = mode_integer.to_s(8)
	rescue
		''
	end
end

def file_ctime(file)
	begin
		File.ctime(file).to_i
	rescue
		0
	end
end

def file_mtime(file)
	begin
		File.mtime(file).to_i
	rescue
		0
	end
end

# def file_atime(file)
# 	begin
# 		File.atime(file).to_i
# 	rescue
# 		0
# 	end
# end

def file_owner(file)
	begin
		uid = File.stat(file).uid
		owner_name = Etc.getpwuid(uid).name
	rescue
		'unknown'
	end
end

def file_group(file)
	begin
		gid = File.stat(file).gid
		group_name = Etc.getgrgid(gid).name
	rescue
		'unknown'
	end
end


def write_object(file, object)
	# file.write object.to_json + "\n\n"
	string = object.to_json
	length = string.size
	# puts "length: #{length}"
	# puts "string: #{string}"
	BinData::Int32be.new(length).write(file)
	file.write string
end

def read_object(file)
	length = BinData::Int32be.new.read(file)
	# puts "length: #{length}"
	string = file.read(length)
	# puts "string: #{string}"
	object = JSON.parse(string)
	return object
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
def scan_recursive(index_file, scan_info, dir_path)
  base_path = Pathname.new(dir_path)

  # write initial dir record
  dir_info_initial = {
  	'type' => 'dir',
  	'dir_path' => dir_path,
  	'name' => File.basename(dir_path),
  	'mode' => file_mode(dir_path),
  	'ctime' => file_ctime(dir_path),
  	'mtime' => file_mtime(dir_path),
  	# 'atime' => file_atime(dir_path),
  	'owner' => file_owner(dir_path),
  	'group' => file_group(dir_path),
  }

  write_object(index_file, dir_info_initial)

  symlinks = []
  dirs = []
  files = []
	content_size = 0
	# content_hash_src = ''
	# meta_hash_src = ''
	content_hashes = []
	meta_hashes = []

  Dir[File.join(base_path, '{*,.*}')].each do |full_path|
    name = Pathname.new(full_path).relative_path_from(base_path).to_s # .to_s converts from Pathname to actual string
    case name
    when '.'	# current dir
    when '..'	# parent dir
    else

			if File.symlink?(full_path)
				symlinks << name

				# get the sie of the symlink itself (not the size of what it's pointing at)
		    size = File.lstat(full_path).size
				symlink_info = {
			  	'type' => 'symlink',
			  	'name' => name,
			  	'link_path' => File.readlink(full_path),
			  	'size' => size,
			  	'mode' => file_mode(full_path),
			  	'ctime' => file_ctime(full_path),
			  	'mtime' => file_mtime(full_path),
			  	# 'atime' => file_atime(full_path),
			  	'owner' => file_owner(full_path),
			  	'group' => file_group(full_path),
			  }
			  symlink_info['hash'] = Hasher.new(scan_info['symlink_hash_template'], symlink_info).hash

			  # accumulate
		    content_size += size
		    meta_hashes << symlink_info['hash']
		    # content_hash does not exist for symlinks

			  write_object(index_file, symlink_info)
	    elsif Dir.exist?(full_path)
	    	# recurse into sub dirs after completing this dir scan, tally things up at the end...
			  dirs << name
			elsif File.exist?(full_path)
				files << name

		    size = File.size(full_path)

		    file_info = {
			  	'type' => 'file',
			  	'name' => name,
			  	'size' => size,
			  	'mode' => file_mode(full_path),
			  	'ctime' => file_ctime(full_path),
			  	'mtime' => file_mtime(full_path),
			  	# 'atime' => file_atime(full_path),
			  	'owner' => file_owner(full_path),
			  	'group' => file_group(full_path),
			  	'md5' => FileHash.md5(full_path),
			  	'sha256' => FileHash.sha256(full_path),
			  }
			  file_info['hash'] = Hasher.new(scan_info['file_hash_template'], file_info).hash

			  # accumulate
		    content_size += size
		    content_hashes << file_info['sha256']
		    meta_hashes << file_info['hash']

			  write_object(index_file, file_info)
			else
				unknown_info = {
			  	'type' => 'unknown',
			  	'name' => name
			  }
			  write_object(index_file, unknown_info)
			end
		end
  end

  # recursive = stats for this dir + stats for all subdirs
  #
  # the properties inside 'recursive' are kept separate from the properties for just the current dir,
  # so that we can report the simple 'this dir only' and also the full recursive status.
	recursive = {
		'content_size' => content_size,
		'symlink_count' => symlinks.count,
		'dir_count' => dirs.count,
		'file_count' => files.count,
		'max_depth' => 0, # 0 means empty dir, 1 means the dir only contains files or symlinks, > 1 indicates subdirs
		'content_hashes' => content_hashes.dup,		# clone array, so 'recursive' can keep adding to it's copy
		'meta_hashes' => meta_hashes.dup,					# clone array, so 'recursive' can keep adding to it's copy
	}


  if dirs.count > 0
	  dirs.each do |dir|
	  	puts "Scanning subdir #{dir} of #{dir_path}"
	  	sub_dir_info = scan_recursive(index_file, scan_info, File.join(dir_path, dir))

	    #
	    # accumulation (for current level onlY)
	    #

	  	# accumulate folder meta-data
	    meta_hashes << sub_dir_info['hash']
	    # content_hash does not make sense for a dir at this level (since we don't recurse, it's effecively empty)

	    #
	    # recursive accumulation
	    #

	  	# accumulate sizes and counts
	  	recursive['content_size'] += sub_dir_info['recursive']['content_size']
	  	recursive['symlink_count'] += sub_dir_info['recursive']['symlink_count']
	  	recursive['dir_count'] += sub_dir_info['recursive']['dir_count']
	  	recursive['file_count'] += sub_dir_info['recursive']['file_count']
	  	recursive['max_depth'] = [recursive['max_depth'], 1 + sub_dir_info['recursive']['max_depth']].max
	  	
	  	# accumulate hash source strings
	    recursive['content_hashes'] << sub_dir_info['recursive']['content_hash']
	    recursive['meta_hashes'] << sub_dir_info['recursive']['meta_hash']
	  end
	else
		max_depth = 1 if files.count > 0 || symlinks.count > 0
	end

	# finalize the hashes
	content_hash_src = content_hashes.join(HASH_SRC_JOIN)
	meta_hash_src = meta_hashes.join(HASH_SRC_JOIN)
	content_hash = StringHash.md5(content_hash_src)
	meta_hash = StringHash.md5(meta_hash_src)

	# finalize the recursive hashes (from their source strings)
	recursive['content_hash_src'] = recursive['content_hashes'].join(HASH_SRC_JOIN)
	recursive['meta_hash_src'] = recursive['meta_hashes'].join(HASH_SRC_JOIN)
	recursive['content_hash'] = StringHash.md5(recursive['content_hash_src'])
	recursive['meta_hash'] = StringHash.md5(recursive['meta_hash_src'])

  # write final dir record
  dir_info_final = dir_info_initial.merge({
		'symlink_count' => symlinks.count,
		'dir_count' => dirs.count,
		'file_count' => files.count,
		'content_size' => content_size,
		# hashes
		'content_hash_src' => content_hash_src,
		'meta_hash_src' => meta_hash_src,
		'content_hash' => content_hash,
		'meta_hash' => meta_hash,
		# recursive summary
		'recursive' => recursive,
	})
 	dir_info_final['hash'] = Hasher.new(scan_info['dir_hash_template'], dir_info_final).hash
  write_object(index_file, dir_info_final)

  # return the summary of all the entries in the folder (including recursive summary)
  return dir_info_final
end

def dir_scan(scan_root, index_path)
	# turn scan_root into the cononical form, making it absolute, with no symlinks
	real_scan_root = Pathname.new(scan_root).realpath

	host_name = Socket.gethostname
	timestamp = 4 # Time.now.to_i
	index_path = File.join real_scan_root, ".dirscan_#{timestamp}" unless index_path

	puts "Host name: #{host_name}"
	puts "Scan root: #{real_scan_root}"
	puts "Index path: #{index_path}"
	
	# create scan object, contains meta-data applicable to the entire scan
	scan_info = {
			'type' => 'dirscan',
			'host_name' => host_name,
			'scan_root' => real_scan_root,
			'timestamp' => Time.now.to_i,
			'index_path' => index_path,
			'symlink_hash_template' => 'name+mode+owner+group+ctime+mtime+size',	# size of symlink itself, not the target
			'file_hash_template' 		=> 'name+mode+owner+group+ctime+mtime+size+sha256',	# size and content hash
			'dir_hash_template' 		=> 'name+mode+owner+group+ctime+mtime+content_size+content_hash+meta_hash',	# size/hash of dir's content
		}

	# create the index file
	File.open(index_path, 'wb') do |index_file|
		# write dirscan meta-data
		write_object(index_file, scan_info)

		# scan recursively
		scan = scan_recursive(index_file, scan_info, real_scan_root)

		puts "scan results:"
		puts scan.to_yaml
	end

	return index_path
end

def report(name, current, recorded)
	if current == recorded
		puts "   OK: #{name} is #{current}"
	else
		puts "ERROR: #{name} is #{current} but recorded was #{recorded}"
	end
end

def scan_verify(index_path)
	# read the index file
	File.open(index_path, 'rb') do |index_file|
		# state objects, updated during parsing
		dirscan = {}
		dir = {}

		object_count = 0
		while not index_file.eof? do
			object = read_object(index_file)
			object_count += 1
			
			case object['type']
			when 'dirscan'
				dirscan = object
			when 'dir'
				dir = object
			when 'symlink'
				symlink = object
				puts "symlink: #{JSON.pretty_generate(symlink)}"
			when 'file'
				file = object
				full_path = File.join dir['dir_path'], file['rel_path']
				if File.exist?(full_path)
					puts "   OK: file '#{full_path}' exists"
					size = File.size(full_path)
					report(:size, size, file['size'])
					report(:md5, FileHash.md5(full_path), file['md5'])
					report(:sha256, FileHash.sha256(full_path), file['sha256'])
				else
					puts "ERROR: file '#{full_path}' not found"
				end
			end
		end

		puts "object_count: #{object_count}"
	end
end

def index_unpack(index_path, text_path)
	# write the text file
	File.open(text_path, 'w') do |text_file|
		# read the index file
		File.open(index_path, 'rb') do |index_file|
			object_count = 0
			while not index_file.eof? do
				object = read_object(index_file)
				text_file.write JSON.pretty_generate(object) + "\n"
				object_count += 1
				# puts 'object: ' + object.to_yaml
			end

			puts "object_count: #{object_count}"
		end
	end
end

# command line arguments
case ARGV[0]
when 's'
	# scan dir, creates an index file
	scan_root = ARGV[1]
	index_path = ARGV[2]
	index_path = dir_scan(scan_root, index_path)
when 'v'
	# verify index file, compares with original dir
	index_path = ARGV[1]
	scan_verify(index_path)
when 'u'
	# unpack index file, writes a text version of the file
	index_path = ARGV[1]
	text_path = ARGV[2]
	index_unpack(index_path, text_path)
end

