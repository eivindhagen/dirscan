require 'open3'
require 'time'

class PathInfo
	def initialize(file_path)
		@file_path = file_path
	end

	def mode
		begin
			mode_integer = File.lstat(@file_path).mode & 0777
			mode_octal_string = mode_integer.to_s(8)
		rescue
			''
		end
	end

	# get the original creation time of a file, using an external process to run 'mdls'
	# WARNING: This is really slow on OSX! It's takes about 4,000 TIMES LONGER than File.mtime()
	def create_time
		begin
			# TODO: check which operating system we are on, and get this value accordingly....

			# File.ctime(@file_path).to_i	# NO NO NO, This is the 'change' time, not the 'create' time :-()

			# OSX stored the birth time of a file in special metadata that Ruby does not have easy access to,
			# so start the 'mdls' command in a separate process to extract this info.
			# WARNING: This is really slow! It's takes about 4,000 TIMES LONGER than File.mtime()
			return Time.parse(Open3.popen3(
				"mdls", 
				"-name",
				"kMDItemContentCreationDate", 
				"-raw", @file_path)[1].read
			).to_i
		rescue
			0
		end
	end

	def modify_time
		begin
			File.mtime(@file_path).to_i
		rescue
			0
		end
	end

	def access_time
		begin
			File.atime(@file_path).to_i
		rescue
			0
		end
	end

	def owner
		begin
			uid = File.stat(@file_path).uid
			owner_name = Etc.getpwuid(uid).name
		rescue
			'unknown'
		end
	end

	def group
		begin
			gid = File.stat(@file_path).gid
			group_name = Etc.getgrgid(gid).name
		rescue
			'unknown'
		end
	end

	def size
		begin
			File.size(@file_path)
		rescue
			nil
		end
	end
end
