#include the test helper
require File.expand_path('test_helper.rb', File.dirname(__FILE__))

#include the classes we are testing
require File.expand_path('../filepile.rb', File.dirname(__FILE__))

# These tests cover the high-level operation of the system, at the 'FilePile' level.
# The process of 'capturing' a folder and storing it in a local FilePile involves the following:
# 1. Scan the folder (hierarchically) and record the current directory info for each file & folder, store in temp DB.
# 2. Compare temp DB to FilePile DB, remove any entries that are 100% identical (modification-time is trusted as sign that file is not modified)
# 3. Generate sha256 hash for each file that remains (only those files that were not found in the FilePile DB).
# 4. Mark files as already stored if the FilePile DB contains the newly generated sha256 value (no need to store the file, even though it seemed new).
# 5. Store remaining files in the FilePile (those that the FilePile DB did not have already).
# 6. Merge temp DB into FilePile DB (after all the file-data has already been stored).
# 7. Cleanup, removed temp DB and any other temporary files.

class TestFilePile < Test::Unit::TestCase

  def test_scan_to_temp_db
    src = 'test_data/nested1_mixed'
    assert(Dir.exist?(src), "Source dir #{src} should exist")

    dst = 'tmp/filepiles/nested1_mixed'
    FileUtils.rm_rf(dst)
    assert(!Dir.exist?(dst), "Destination dir #{dst} should not exist")

    command = Command.new
    command.execute(['store', src, dst, '-log', 'warn'])  

    assert(Dir.exist?(src), "Source dir #{src} should still exist")
    assert(Dir.exist?(dst), "Destination dir #{dst} should exist")

    # there should be a database file in the metadata/ folder
    dst_db_path = File.join(dst, 'metadata', 'db.sqlite3')
    assert(File.exist?(dst_db_path), "Destination DB #{dst_db_path} should exist")
  end
      
end
