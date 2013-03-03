#include the test helper
require File.expand_path('test_helper.rb', File.dirname(__FILE__))

#include the classes we are testing
require File.expand_path('../lib/file_info_db.rb', File.dirname(__FILE__))

class TestIndexFile < Test::Unit::TestCase

  def add_file_record(id)
    FileInfo.create({
      type: 1,
      name: "name#{id}",
      size: 100 + id,
      mode: '777',
      mtime: 1000000 + id,
      own: 'abe',
      grp: 'abe',
      sha256: 'asdf0987asdf0987asdf0987asdf9087asdf0987asdf0987asdf0987asdf9087',
      path: '/users/abe/',
    })
  end

  def test_single_record
    # path to temp file for this test
    path = File.expand_path("../tmp/tmp_test.db", File.dirname(__FILE__))
    File.delete(path) if File.exist?(path)

    # create database file
    FileInfoDb.new(path)
    assert_equal(0, FileInfo.count)

    # add a record
    add_file_record(1)
    assert_equal(1, FileInfo.count)

    # reopen database
    FileInfoDb.new(path)
    assert_equal(1, FileInfo.count)

    # reopen database
    rec = FileInfo.get(1)
    rec.destroy

    # delete the temp file
    File.delete(path)
    assert_equal(false, File.exist?(path) ) # file should not exist
  end

end
