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
    inputs = {:scan_root => 'test_data/empty'}
    outputs = {}
    ds = DirScanner.new(inputs, outputs)
    assert_equal('test_data/empty', ds.inputs[:scan_root])
    assert_equal(nil, ds.inputs[:index_path])
    assert_equal(nil, ds.inputs[:timestamp])
  end
   
  def test_onefile_full
    # scan
    inputs = {:scan_root => 'test_data/one_file'}
    outputs = {:scan_index => 'tmp/one_file.dirscan'}
    ds = DirScanner.new(inputs, outputs)
    scan_result = ds.scan

    # unpack the scan
    inputs = {:scan_index => 'tmp/one_file.dirscan'}
    outputs = {:scan_unpack => 'tmp/one_file.json'}
    ds = DirScanner.new(inputs, outputs)
    unpack_result = ds.unpack

    # extract the scan, verify attributes
    inputs = {:scan_index => 'tmp/one_file.dirscan'}
    outputs = {}
    ds = DirScanner.new(inputs, outputs)
    extract_result = ds.extract
    dir = extract_result[:dirs]["test_data/one_file"]
    assert_not_nil(dir, "directory 'one_file' should be in the extract result")
    if dir
      assert_equal('dir', dir[:type])
      assert_equal('one_file', dir[:name])
      assert_equal('755', dir[:mode])
      # assert_equal(1354779288, dir[:ctime])
      assert_equal(1354956010, dir[:mtime])
      assert_equal('eivindhagen', dir[:owner])
      assert_equal('staff', dir[:group])

      assert_equal(0, dir[:symlink_count])
      assert_equal(0, dir[:dir_count])
      assert_equal(1, dir[:file_count])
      assert_equal(3, dir[:content_size])
      assert_equal('7692c3ad3540bb803c020b3aee66cd8887123234ea0c6e7143c0add73ff431ed', dir[:content_hash_src])
      assert_equal('b418a016651e8ebbe31b235b54c22a44256280a61a49e2b2f064e31ea7196db9', dir[:meta_hash_src])
      assert_equal('fbe996f69a7152d7b955498723219f35', dir[:content_hash])
      assert_equal('afffb0bb824b01e5c371b2186da9bd8d', dir[:meta_hash])
      assert_equal("one_file+755+eivindhagen+staff+1354956010+3+fbe996f69a7152d7b955498723219f35+afffb0bb824b01e5c371b2186da9bd8d", dir[:hash_src])
      assert_equal("b202f7e9283f39d54e57c4b9edf8e730666a0e84ecc84a0a6caefc1b7587a188", dir[:hash])

      # recursive
      assert_equal(3, dir[:recursive][:content_size])
      assert_equal(0, dir[:recursive][:symlink_count])
      assert_equal(0, dir[:recursive][:dir_count])
      assert_equal(1, dir[:recursive][:file_count])
      assert_equal(0, dir[:recursive][:max_depth])
      assert_equal(["7692c3ad3540bb803c020b3aee66cd8887123234ea0c6e7143c0add73ff431ed"], dir[:recursive][:content_hashes])
      assert_equal(["b418a016651e8ebbe31b235b54c22a44256280a61a49e2b2f064e31ea7196db9"], dir[:recursive][:meta_hashes])
      assert_equal("7692c3ad3540bb803c020b3aee66cd8887123234ea0c6e7143c0add73ff431ed", dir[:recursive][:content_hash_src])
      assert_equal("b418a016651e8ebbe31b235b54c22a44256280a61a49e2b2f064e31ea7196db9", dir[:recursive][:meta_hash_src])
      assert_equal("fbe996f69a7152d7b955498723219f35", dir[:recursive][:content_hash])
      assert_equal("afffb0bb824b01e5c371b2186da9bd8d", dir[:recursive][:meta_hash])
    end
    file = dir[:entries]['file1.txt']
    assert_not_nil(file, "file 'file1.txt' should be in the extract result")
    if file
      assert_equal('file', file[:type])
      assert_equal('file1.txt', file[:name])
      assert_equal(3, file[:size])
      assert_equal('644', file[:mode])
      # assert_equal(1354956010, file[:ctime])
      assert_equal(1354956047, file[:mtime])
      assert_equal('eivindhagen', file[:owner])
      assert_equal('staff', file[:group])
      assert_equal('f97c5d29941bfb1b2fdab0874906ab82', file[:md5])
      assert_equal('7692c3ad3540bb803c020b3aee66cd8887123234ea0c6e7143c0add73ff431ed', file[:sha256])
      assert_equal("file1.txt+644+eivindhagen+staff+1354956047+3+7692c3ad3540bb803c020b3aee66cd8887123234ea0c6e7143c0add73ff431ed", file[:hash_src])
      assert_equal("b418a016651e8ebbe31b235b54c22a44256280a61a49e2b2f064e31ea7196db9", file[:hash])
    end

    # verify the scan, there should be 0 issues
    inputs = {:scan_index => 'tmp/one_file.dirscan'}
    outputs = {}
    dv = DirScanner.new(inputs, outputs)
    verify_result = ds.verify
    assert_equal(0, verify_result[:issues_count])
  end

  def test_onefile_quick
    # scan
    inputs = {:scan_root => 'test_data/one_file', :quick_scan => true}
    outputs = {:scan_index => 'tmp/one_file.dirscan'}
    ds = DirScanner.new(inputs, outputs)
    scan_result = ds.scan

    # unpack the scan
    inputs = {:scan_index => 'tmp/one_file.dirscan'}
    outputs = {:scan_unpack => 'tmp/one_file.json'}
    ds = DirScanner.new(inputs, outputs)
    unpack_result = ds.unpack

    # extract the scan, verify attributes
    inputs = {:scan_index => 'tmp/one_file.dirscan'}
    outputs = {}
    ds = DirScanner.new(inputs, outputs)
    extract_result = ds.extract
    dir = extract_result[:dirs]["test_data/one_file"]
    assert_not_nil(dir, "directory 'one_file' should be in the extract result")
    if dir
      assert_equal('dir', dir[:type])
      assert_equal('one_file', dir[:name])
      assert_equal('755', dir[:mode])
      assert_nil(dir[:ctime])
      assert_equal(1354956010, dir[:mtime])
      assert_equal('eivindhagen', dir[:owner])
      assert_equal('staff', dir[:group])

      assert_equal(0, dir[:symlink_count])
      assert_equal(0, dir[:dir_count])
      assert_equal(1, dir[:file_count])
      assert_equal(3, dir[:content_size])

      # recursive
      assert_equal(3, dir[:recursive][:content_size])
      assert_equal(0, dir[:recursive][:symlink_count])
      assert_equal(0, dir[:recursive][:dir_count])
      assert_equal(1, dir[:recursive][:file_count])
      assert_equal(0, dir[:recursive][:max_depth])
    end
    file = dir[:entries]['file1.txt']
    assert_not_nil(file, "file 'file1.txt' should be in the extract result")
    if file
      assert_equal('file', file[:type])
      assert_equal('file1.txt', file[:name])
      assert_equal(3, file[:size])
      assert_equal('644', file[:mode])
      # assert_equal(1354956010, file[:ctime])
      assert_equal(1354956047, file[:mtime])
      assert_equal('eivindhagen', file[:owner])
      assert_equal('staff', file[:group])
    end

    # verify the scan, there should be 0 issues
    inputs = {:scan_index => 'tmp/one_file.dirscan'}
    outputs = {}
    ds = DirScanner.new(inputs, outputs)
    verify_result = ds.verify
    assert_equal(0, verify_result[:issues_count])

    # analyze the scan
    inputs = {:scan_index => 'tmp/one_file.dirscan'}
    outputs = {:analysis => 'tmp/one_file.dirscan.analysis'}
    ds = DirScanner.new(inputs, outputs)
    analysis_result = ds.analyze
    assert_equal({:file_sizes=>{3=>1}}, analysis_result)

    # analysis report
    inputs = {:analysis => 'tmp/one_file.dirscan.analysis'}
    outputs = {:analysis_report => 'tmp/one_file.dirscan.analysis.report'}
    ds = DirScanner.new(inputs, outputs)
    analysis_report = ds.analysis_report
    assert_equal({:sorted_by_count=>[[3, 1]]}, analysis_report)

  end
   
end
