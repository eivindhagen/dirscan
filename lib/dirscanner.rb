require File.join(File.dirname(__FILE__), 'hasher')
require File.join(File.dirname(__FILE__), 'pathinfo')
require File.join(File.dirname(__FILE__), 'indexfile')

require 'pathname'
require 'json'
require 'bindata'
require 'socket'

class DirScanner
	attr_accessor :scan_root, :index_path, :timestamp, :scan_info, :scan_result

	def initialize(options = {})
		@options = options
		@scan_root = options[:scan_root]		# required for scan()
		@index_path = options[:index_path]	# required for verify() & unpack(). scan() will create index_path if not provided
		@timestamp = options[:timestamp]		# optional, will be set to current time if not provided
	end

	# perform a directory scan, by inspecting all files, symlinks and folders (recursively)
	# requires that @scan_root and @index_file has already been set
	def scan()
		throw ":scan_root is not set" unless @scan_root

		@timestamp = Time.now.to_i unless @timestamp
		
		# index file will be inside the scan_root foler, unless otherwise specified
		@index_path = File.join(real_scan_root, ".dirscan_#{timestamp}") unless @index_path

		host_name = Socket.gethostname
		
		# create scan object, contains meta-data for the entire scan
		@scan_info = {
				:type => :dirscan,
				:host_name => host_name,
				:scan_root => @scan_root,
				:scan_root_real => Pathname.new(@scan_root).realpath,	# turn scan_root into the canonical form, making it absolute, with no symlinks
				:timestamp => timestamp,
				:index_path => index_path,

				# these templates specify how to create the hash source strings for various dir entry types
				:symlink_hash_template => 'name+mode+owner+group+ctime+mtime+size'.freeze,	# size of symlink itself, not the target
				:file_hash_template 		=> 'name+mode+owner+group+ctime+mtime+size+sha256'.freeze,	# size and content hash
				:dir_hash_template 		=> 'name+mode+owner+group+ctime+mtime+content_size+content_hash+meta_hash'.freeze,	# size/hash of dir's content
			}

		# create the index file, and perform the scan
		IndexFile::Writer.new(@index_path) do |index_file|
			# write dirscan meta-data
			index_file.write_object(scan_info)

			# scan recursively
			@scan_result = scan_recursive(index_file, @scan_info, @scan_root)
		end

		return @scan_result
	end

	def verify()
		throw ":index_path is not set" unless @index_path

		object_count = 0
		issues_count = 0

		# read the index file
		IndexFile::Reader.new(@index_path) do |index_file|
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
					full_path = File.join dir[:dir_path], file[:name]
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


	def extract()
		throw ":index_path is not set" unless @index_path

		result = {
			:dirscan => nil,
			:dirs => {},
		}
		object_count = 0

		# read the index file
		IndexFile::Reader.new(@index_path) do |index_file|
			# state objects, updated during parsing
			dir = nil

			# read from index file until we reach the end
			while not index_file.eof? do
				object = index_file.read_object
				puts "object: #{object.inspect}"
				object_count += 1
				
				case object[:type].to_sym
				when :dirscan
					result[:dirscan] = object
				when :dir
					dir = object
					# store in result, or merge it if an initial record already exist
					dir_path = dir[:dir_path]
					if result[:dirs][dir_path]
						result[:dirs][dir_path].merge! dir
					else
						result[:dirs][dir_path] = dir
						# prep dir to hold child entries
						dir[:entries] ||= {}
					end

					# set current dir
					dir = result[:dirs][dir_path]
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


	def unpack(text_file_path)
		throw ":index_path is not set" unless @index_path

		# read the index file
		IndexFile::Reader.new(index_path) do |index_file|
			# write the text file
			File.open(text_file_path, 'w') do |text_file|
				while not index_file.eof? do
					object = index_file.read_object
					text_file.write JSON.pretty_generate(object) + "\n"
				end
			end
		end
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

	  pathinfo = PathInfo.new(dir_path)

	  # write initial dir record
	  dir_info_initial = {
	  	:type => :dir,
	  	:dir_path => dir_path,
	  	:name => File.basename(dir_path),
	  	:mode => pathinfo.mode,
	  	:ctime => pathinfo.create_time,
	  	:mtime => pathinfo.modify_time,
	  	:owner => pathinfo.owner,
	  	:group => pathinfo.group,
	  }

	  index_file.write_object(dir_info_initial)

	  symlinks = []
	  dirs = []
	  files = []
		content_size = 0
		# content_hash_src = ''
		# meta_hash_src = ''
		content_hashes = []
		meta_hashes = []

	  Dir[File.join(base_path, '{*,.*}')].each do |full_path|
	  	pathinfo = PathInfo.new(full_path)
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
				  	:type => :symlink,
				  	:name => name,
				  	:link_path => File.readlink(full_path),
				  	:size => size,
				  	:mode => pathinfo.mode,
				  	:ctime => pathinfo.create_time,
				  	:mtime => pathinfo.modify_time,
				  	:owner => pathinfo.owner,
				  	:group => pathinfo.group,
				  }
				  hasher = Hasher.new(scan_info[:symlink_hash_template], symlink_info)
				  symlink_info[:hash_src] = hasher.source
				  symlink_info[:hash] = hasher.hash

				  # accumulate
			    content_size += size
			    meta_hashes << symlink_info[:hash]
			    # content_hash does not exist for symlinks

				  index_file.write_object(symlink_info)
		    elsif Dir.exist?(full_path)
		    	# recurse into sub dirs after completing this dir scan, tally things up at the end...
				  dirs << name
				elsif File.exist?(full_path)
					files << name

			    size = File.size(full_path)

			    file_info = {
				  	:type => :file,
				  	:name => name,
				  	:size => size,
				  	:mode => pathinfo.mode,
				  	:ctime => pathinfo.create_time,
				  	:mtime => pathinfo.modify_time,
				  	:owner => pathinfo.owner,
				  	:group => pathinfo.group,
				  	:md5 => FileHash.md5(full_path),
				  	:sha256 => FileHash.sha256(full_path),
				  }
				  hasher = Hasher.new(scan_info[:file_hash_template], file_info)
				  file_info[:hash_src] = hasher.source
				  file_info[:hash] = hasher.hash

				  # accumulate
			    content_size += size
			    content_hashes << file_info[:sha256]
			    meta_hashes << file_info[:hash]

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
			:content_hashes => content_hashes.dup,		# clone array, so 'recursive' can keep adding to it's copy
			:meta_hashes => meta_hashes.dup,					# clone array, so 'recursive' can keep adding to it's copy
		}


	  if dirs.count > 0
		  dirs.each do |dir|
		  	puts "Scanning subdir #{dir} of #{dir_path}"
		  	sub_dir_info = scan_recursive(index_file, scan_info, File.join(dir_path, dir))

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
		    recursive[:content_hashes] << sub_dir_info[:recursive][:content_hash]
		    recursive[:meta_hashes] << sub_dir_info[:recursive][:meta_hash]
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
		recursive[:content_hash_src] = recursive[:content_hashes].join(HASH_SRC_JOIN)
		recursive[:meta_hash_src] = recursive[:meta_hashes].join(HASH_SRC_JOIN)
		recursive[:content_hash] = StringHash.md5(recursive[:content_hash_src])
		recursive[:meta_hash] = StringHash.md5(recursive[:meta_hash_src])

	  # write final dir record
	  dir_info_final = dir_info_initial.merge({
			:symlink_count => symlinks.count,
			:dir_count => dirs.count,
			:file_count => files.count,
			:content_size => content_size,
			# hashes
			:content_hash_src => content_hash_src,
			:meta_hash_src => meta_hash_src,
			:content_hash => content_hash,
			:meta_hash => meta_hash,
			# recursive summary
			:recursive => recursive,
		})
	  hasher = Hasher.new(scan_info[:dir_hash_template], dir_info_final)
	  dir_info_final[:hash_src] = hasher.source
	  dir_info_final[:hash] = hasher.hash

	  index_file.write_object(dir_info_final)

	  # return the summary of all the entries in the folder (including recursive summary)
	  return dir_info_final
	end

end

