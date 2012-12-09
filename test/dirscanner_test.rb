# unit_tests.rb
#
# http://en.wikibooks.org/wiki/Ruby_Programming/Unit_testing
#
require "test/unit"
require 'redgreen'

#include the classes we are testing
require File.expand_path('../lib/dirscanner.rb', File.dirname(__FILE__))

class TestDirScanner < Test::Unit::TestCase

	def test_create
		ds = DirScanner.new(:scan_root => '../test_data/empty')
		assert_equal('../test_data/empty', ds.scan_root)
		assert_equal(nil, ds.index_path)
		assert_equal(nil, ds.timestamp)
	end
	 
	def test_onefile
		# scan
		ds = DirScanner.new(
			:scan_root => 'test_data/one_file',
			:index_path => 'tmp/one_file.dirscan',
		)
		scan_result = ds.scan

		# unpack the scan
		du = DirScanner.new(
			:index_path => 'tmp/one_file.dirscan',
		)
		unpack_path = 'tmp/one_file.json'
		unpack_result = ds.unpack(unpack_path)

		# extract the scan, verify attributes
		de = DirScanner.new(
			:index_path => 'tmp/one_file.dirscan',
		)
		extract_result = ds.extract
		puts "extract_result: #{extract_result.inspect}"
		dir = extract_result[:dirs]["test_data/one_file"]
		assert_not_nil(dir, "directory 'one_file' should be in the extract result")
		if dir
			assert_equal('dir', dir[:type])
			assert_equal('one_file', dir[:name])
			assert_equal('755', dir[:mode])
			assert_equal(1354779288, dir[:ctime])
			assert_equal(1354956010, dir[:mtime])
			assert_equal('eivindhagen', dir[:owner])
			assert_equal('staff', dir[:group])

			assert_equal(0, dir[:symlink_count])
  		assert_equal(0, dir[:dir_count])
  		assert_equal(1, dir[:file_count])
  		assert_equal(3, dir[:content_size])
  		assert_equal('7692c3ad3540bb803c020b3aee66cd8887123234ea0c6e7143c0add73ff431ed', dir[:content_hash_src])
  		assert_equal('7f8e1ec1c655fa111d605cc3e1860eb96750c99c1ffe683309cd5f63f0446912', dir[:meta_hash_src])
  		assert_equal('fbe996f69a7152d7b955498723219f35', dir[:content_hash])
  		assert_equal('2aefc9464c941f82827ee0f304ef8162', dir[:meta_hash])

  		# recursive
    	assert_equal(3, dir[:recursive][:content_size])
    	assert_equal(0, dir[:recursive][:symlink_count])
    	assert_equal(0, dir[:recursive][:dir_count])
    	assert_equal(1, dir[:recursive][:file_count])
    	assert_equal(0, dir[:recursive][:max_depth])
    	assert_equal(["7692c3ad3540bb803c020b3aee66cd8887123234ea0c6e7143c0add73ff431ed"], dir[:recursive][:content_hashes])
    	assert_equal(["7f8e1ec1c655fa111d605cc3e1860eb96750c99c1ffe683309cd5f63f0446912"], dir[:recursive][:meta_hashes])
    	assert_equal("7692c3ad3540bb803c020b3aee66cd8887123234ea0c6e7143c0add73ff431ed", dir[:recursive][:content_hash_src])
    	assert_equal("7f8e1ec1c655fa111d605cc3e1860eb96750c99c1ffe683309cd5f63f0446912", dir[:recursive][:meta_hash_src])
    	assert_equal("fbe996f69a7152d7b955498723219f35", dir[:recursive][:content_hash])
    	assert_equal("2aefc9464c941f82827ee0f304ef8162", dir[:recursive][:meta_hash])
		end
		file = dir[:entries]['file1.txt']
		assert_not_nil(file, "file 'file1.txt' should be in the extract result")
		if file
			assert_equal('file', file[:type])
			assert_equal('file1.txt', file[:name])
			assert_equal(3, file[:size])
			assert_equal('644', file[:mode])
			assert_equal(1354956010, file[:ctime])
			assert_equal(1354956047, file[:mtime])
			assert_equal('eivindhagen', file[:owner])
			assert_equal('staff', file[:group])
			assert_equal('f97c5d29941bfb1b2fdab0874906ab82', file[:md5])
			assert_equal('7692c3ad3540bb803c020b3aee66cd8887123234ea0c6e7143c0add73ff431ed', file[:sha256])
			assert_equal('7f8e1ec1c655fa111d605cc3e1860eb96750c99c1ffe683309cd5f63f0446912', file[:hash])
		end

		# verify the scan, there should be 0 issues
		dv = DirScanner.new(
			:index_path => 'tmp/one_file.dirscan',
		)
		verify_result = ds.verify
		assert_equal(0, verify_result[:issues_count])
	end
	 
end
