# dirscan.rb

require 'socket'
require 'pathname'
require 'json'
require 'yaml'
require 'BinData'

def file_md5(file_path)
	require 'digest/md5'
	Digest::MD5.file(file_path).hexdigest
  # digest = Digest::MD5.hexdigest(File.read(file_path))
end

def file_sha256(file_path)
	Digest::SHA256.file(file_path).hexdigest
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

def scan_recursive(index_file, dir_path)
  base_path = Pathname.new(dir_path)
  write_object(index_file, {'type' => 'dir', 'dir_path' => dir_path})

  symlinks = []
  dirs = []
  files = []
	content_size = 0

	recursive = {
		'content_size' => 0,
		'symlink_count' => 0,
		'dir_count' => 0,
		'file_count' => 0,
		'max_depth' => 0, # 0 means empty dir, 1 means the dir only contains files or symlinks, > 1 indicates subdirs
	}

  Dir[File.join(base_path, '{*,.*}')].each do |full_path|
    rel_path = Pathname.new(full_path).relative_path_from(base_path).to_s # .to_s converts from Pathname to actual string
    case rel_path
    when '.'	# current dir
    when '..'	# parent dir
    else

			if File.symlink?(full_path)
				symlinks << rel_path

				# get the sie of the symlink itself (not the size of what it's pointing at)
		    size = File.lstat(full_path).size
		    content_size += size
				
			  write_object(index_file, {
			  	'type' => 'symlink',
			  	'size' => size,
			  	'rel_path' => rel_path,
			  	'link_path' => File.readlink(full_path),
			  })
	    elsif Dir.exist?(full_path)
	    	# recurse into sub dirs after completing this dir scan
			  dirs << rel_path
			elsif File.exist?(full_path)
				files << rel_path

				# get this size of the file
		    size = File.size(full_path)
		    content_size += size

			  write_object(index_file, {
			  	'type' => 'file',
			  	'size' => size,
			  	'md5' => file_md5(full_path),
			  	'sha256' => file_sha256(full_path),
			  	'rel_path' => rel_path
			  })
			else
			  write_object(index_file, {
			  	'type' => 'unknown',
			  	'rel_path' => rel_path
			  })
			end
		end
  end

  # recursive stats = stats for this dir + stats for all subdirs
	recursive['content_size'] = content_size
	recursive['symlink_count'] = symlinks.count
	recursive['dir_count'] = dirs.count
	recursive['file_count'] = files.count

  if dirs.count > 0
	  dirs.each do |dir|
	  	puts "Scanning subdir #{dir} of #{dir_path}"
	  	scan = scan_recursive(index_file, File.join(dir_path, dir))
	  	recursive['content_size'] += scan['recursive']['content_size']
	  	recursive['symlink_count'] += scan['recursive']['symlink_count']
	  	recursive['dir_count'] += scan['recursive']['dir_count']
	  	recursive['file_count'] += scan['recursive']['file_count']
	  	recursive['max_depth'] = [recursive['max_depth'], 1 + scan['recursive']['max_depth']].max
	  end
	else
		max_depth = 1 if files.count > 0 || symlinks.count > 0
	end

  # write a new record that contains the content_size
  write_object(
  	index_file, {
  		'type' => 'dir',
  		'dir_path' => dir_path,
  		'content_size' => content_size,
  		'recursive' => recursive
  	}
  )

  # return the total size of all the files in the folder (recursively)
  return 	result = {
		'symlink_count' => symlinks.count,
		'dir_count' => dirs.count,
		'file_count' => files.count,
		'content_size' => content_size,
		'recursive' => recursive,
	}
end

def dir_scan(scan_root)
	# turn scan_root into the cononical form, making it absolute, with no symlinks
	real_scan_root = Pathname.new(scan_root).realpath

	host_name = Socket.gethostname
	timestamp = 3 # Time.now.to_i
	index_path = File.join real_scan_root, ".dirscan_#{timestamp}"

	puts "Host name: #{host_name}"
	puts "Scan root: #{real_scan_root}"
	puts "Index path: #{index_path}"
	
	# create the index file
	File.open(index_path, 'wb') do |index_file|
		# write dirscan meta-data
		write_object(index_file, {
			'type' => 'dirscan',
			'host_name' => host_name,
			'scan_root' => real_scan_root,
			'timestamp' => Time.now.to_i,
			'index_path' => index_path,
		})

		# scan recursively
		scan = scan_recursive(index_file, real_scan_root)

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
					report(:md5, file_md5(full_path), file['md5'])
					report(:sha256, file_sha256(full_path), file['sha256'])
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
	index_path = dir_scan(scan_root)
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



