#include the test helper
require File.expand_path('test_helper.rb', File.dirname(__FILE__))

#include the classes we are testing
require File.expand_path('../lib/pathinfo.rb', File.dirname(__FILE__))

class TestPathInfo < Test::Unit::TestCase

  def test_file
    pathinfo = PathInfo.new(File.expand_path('../test_data/one_file/file1.txt', File.dirname(__FILE__)))
    assert_equal('644', pathinfo.mode )
    assert_equal('eivindhagen', pathinfo.owner )
    assert_equal('staff', pathinfo.group )
    assert_equal(3, pathinfo.size )
    assert_equal(1354956010, pathinfo.create_time ) # this test is volatile, the file's create time will change if the file is deleted/restored
    assert_equal(1354956047, pathinfo.modify_time ) # this test is volatile, the file's modify tile will change if the file is edited
    # assert_equal(1354956078, pathinfo.access_time ) # this test is volatile, because the file's access time changes when it's read
  end

  def test_perf_create_time
    require "benchmark"

    pathinfo = PathInfo.new(File.expand_path('../test_data/one_file/file1.txt', File.dirname(__FILE__)))
    ops = 10

    time = Benchmark.realtime do
      ops.times do
        pathinfo.create_time
      end
    end
    time_per_op = time / ops
    # logger.debug "create_time: #{time_per_op*1000000} us/op"
    assert(time_per_op < 0.1)
  end
   
  def test_perf_modify_time
    require "benchmark"

    pathinfo = PathInfo.new(File.expand_path('../test_data/one_file/file1.txt', File.dirname(__FILE__)))
    ops = 10

    time = Benchmark.realtime do
      ops.times do
        pathinfo.modify_time
      end
    end
    time_per_op = time / ops
    # logger.debug "modify_time: #{time_per_op*1000000} us/op"
    assert(time_per_op < 0.1)
  end
   
end
