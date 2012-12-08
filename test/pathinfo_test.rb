# unit_tests.rb
#
# http://en.wikibooks.org/wiki/Ruby_Programming/Unit_testing
#
require "test/unit"
require 'redgreen'

#include the classes we are testing
require File.expand_path('../lib/pathinfo.rb', File.dirname(__FILE__))

class TestPathInfo < Test::Unit::TestCase

	def test_file
		pathinfo = PathInfo.new(File.expand_path('../test_data/one_file/file1.txt', File.dirname(__FILE__)))
    assert_equal('644', pathinfo.mode )
    assert_equal('eivindhagen', pathinfo.owner )
    assert_equal('staff', pathinfo.group )
    assert_equal(3, pathinfo.size )
    assert_equal(1354956010, pathinfo.create_time )	# this test is volatile, the file's create time will change if the file is deleted/restored
    assert_equal(1354956047, pathinfo.modify_time )	# this test is volatile, the file's modify tile will change if the file is edited
    # assert_equal(1354956078, pathinfo.access_time )	# this test is volatile, because the file's access time changes when it's read
	end
	 
end
