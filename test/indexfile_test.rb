#include the test helper
require File.expand_path('test_helper.rb', File.dirname(__FILE__))

#include the classes we are testing
require File.expand_path('../lib/indexfile.rb', File.dirname(__FILE__))
require File.expand_path('../lib/pathinfo.rb', File.dirname(__FILE__))

class TestIndexFile < Test::Unit::TestCase

  def test_one_object
    # path to temp file for this test
    path = File.expand_path("../tmp/tmp_test.indexfile", File.dirname(__FILE__))

    # create objects in outer scope
    obj1, obj2 = {}

    # write an object to the index file
    IndexFile::Writer.new(path) do |indexfile|
      obj1 = {:rand => 1000 + rand(1000)}
      indexfile.write_object(obj1)
    end

    # verify size of index file
    pathinfo = PathInfo.new(path)
    assert_equal(17, pathinfo.size )

    # read the object in from teh file
    IndexFile::Reader.new(path) do |indexfile|
      obj2 = indexfile.read_object
    end

    # verify that objects are identical
    assert_equal(obj1, obj2)

    # delete the temp file
    File.delete(path)
    pathinfo = PathInfo.new(path)
    assert_equal(nil, pathinfo.size ) # file should not exist
  end
   
  def test_object_recursive
    # path to temp file for this test
    path = File.expand_path("../tmp/tmp_test.indexfile", File.dirname(__FILE__))

    # create objects in outer scope
    obj1, obj2 = {}

    # write an object to the index file
    IndexFile::Writer.new(path) do |indexfile|
      obj1 = {:one => 1, :recursive => {:two => 2}}
      indexfile.write_object(obj1)
    end

    # read the object in from the file
    IndexFile::Reader.new(path) do |indexfile|
      obj2 = indexfile.read_object
    end

    # verify that objects are identical
    assert_equal(obj1, obj2)

    # delete the temp file
    File.delete(path)
    pathinfo = PathInfo.new(path)
    assert_equal(nil, pathinfo.size ) # file should not exist
  end
   
end
