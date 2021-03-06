#include the test helper
require File.expand_path('test_helper.rb', File.dirname(__FILE__))

#include helper classes
require File.expand_path('../lib/assert_json_file_contains.rb', File.dirname(__FILE__))

#include the classes we are testing
require File.expand_path('../lib/dirscan_worker.rb', File.dirname(__FILE__))

class TestDirScanWorker < Test::Unit::TestCase

  def test_create
    inputs = {:scan_root => 'test_data/empty'}
    outputs = {}
    ds = DirScanWorker.new(inputs, outputs)
    assert_equal('test_data/empty', ds.inputs[:scan_root])
    assert_equal(nil, ds.inputs[:index_path])
    assert_equal(nil, ds.inputs[:timestamp])
  end
   
  # full scan collects per-file content hashes, which is slow, but makes it easy to find exact duplicates later
  def test_onefile_full
    # scan
    inputs = {files: {scan_root: 'test_data/one_file'}}
    outputs = {files: {scan_index: 'tmp/one_file.dirscan'}}
    ds = DirScanWorker.new(inputs, outputs)
    scan_result = ds.scan

    # unpack the scan
    inputs = {files: {scan_index: 'tmp/one_file.dirscan'}}
    outputs = {files: {scan_unpack: 'tmp/one_file.json'}}
    ds = DirScanWorker.new(inputs, outputs)
    unpack_result = ds.unpack

    # extract the scan, verify attributes
    inputs = {files: {scan_index: 'tmp/one_file.dirscan'}}
    outputs = {}
    ds = DirScanWorker.new(inputs, outputs)
    extract_result = ds.extract
    assert_equal('test_data/one_file', extract_result[:dirscan][:scan_root])

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
    inputs = {files: {scan_index: 'tmp/one_file.dirscan'}}
    outputs = {}
    dv = DirScanWorker.new(inputs, outputs)
    verify_result = ds.verify
    assert_equal(0, verify_result[:issues_count])
  end

  # quick scan omits the per-file content hashes, instead the laters processes will hash same-size files
  # in order to locate duplicates more efficiently
  def test_onefile_quick
    # scan
    inputs = {files: {scan_root: 'test_data/one_file'}, values: {quick_scan: true}}
    outputs = {files: {scan_index: 'tmp/one_file_quick.dirscan'}}
    ds = DirScanWorker.new(inputs, outputs)
    scan_result = ds.scan

    # unpack the scan
    inputs = {files: {scan_index: 'tmp/one_file_quick.dirscan'}}
    outputs = {files: {scan_unpack: 'tmp/one_file_quick.json'}}
    ds = DirScanWorker.new(inputs, outputs)
    unpack_result = ds.unpack

    # extract the scan, verify attributes
    inputs = {files: {scan_index: 'tmp/one_file_quick.dirscan'}}
    outputs = {}
    ds = DirScanWorker.new(inputs, outputs)
    extract_result = ds.extract
    # puts "extract_result: #{extract_result}"
    assert_equal('test_data/one_file', extract_result[:dirscan][:scan_root])

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
    inputs = {files: {scan_index: 'tmp/one_file_quick.dirscan'}}
    outputs = {}
    ds = DirScanWorker.new(inputs, outputs)
    verify_result = ds.verify
    assert_equal(0, verify_result[:issues_count])

    # analyze the scan
    inputs = {files: {scan_index: 'tmp/one_file_quick.dirscan'}}
    outputs = {files: {analysis: 'tmp/one_file_quick.dirscan.analysis'}}
    ds = DirScanWorker.new(inputs, outputs)
    analysis_result = ds.analyze
    assert_equal({:file_sizes=>{3=>1}, :dir_sizes=>{3=>1}}, analysis_result)

    # analysis report
    inputs = {files: {analysis: 'tmp/one_file_quick.dirscan.analysis'}}
    outputs = {files: {analysis_report: 'tmp/one_file_quick.dirscan.analysis.report'}}
    ds = DirScanWorker.new(inputs, outputs)
    analysis_report = ds.analysis_report
    assert_equal({:sorted_by_count=>[[3, 1]]}, analysis_report)

    # iddupe_files - uses analysis to find exact duplicate files
    inputs = {
      files: {
        scan_index: 'tmp/one_file_quick.dirscan',
        :analysis => 'tmp/one_file_quick.dirscan.analysis',
      }
    }
    outputs = {
      files: {
        iddupe_files: 'tmp/one_file_quick.dirscan.analysis.iddupe_files',
        sha256_cache: 'tmp/one_file_quick.dirscan.sha256_cache',
      }
    }
    ds = DirScanWorker.new(inputs, outputs)
    iddupe_files_result = ds.iddupe_files
    assert_equal({:collection_by_file_size=>{}}, iddupe_files_result, "there should be no dupes")
  end

  # quick scan omits the per-file content hashes, instead the laters processes will hash same-size files
  # in order to locate duplicates more efficiently
  def test_dupes_quick
    # scan
    inputs = {files: {:scan_root => 'test_data/nested1_mixed'}, values: {:quick_scan => true}}
    outputs = {files: {:scan_index => 'tmp/nested1_mixed.dirscan'}}
    ds = DirScanWorker.new(inputs, outputs)
    scan_result = ds.scan

    # unpack the scan
    inputs = {files: {:scan_index => 'tmp/nested1_mixed.dirscan'}}
    outputs = {files: {:scan_unpack => 'tmp/nested1_mixed.json'}}
    ds = DirScanWorker.new(inputs, outputs)
    unpack_result = ds.unpack

    # extract the scan, verify attributes
    inputs = {files: {:scan_index => 'tmp/nested1_mixed.dirscan'}}
    outputs = {}
    ds = DirScanWorker.new(inputs, outputs)
    extract_result = ds.extract

    # analyze the scan
    inputs = {files: {:scan_index => 'tmp/nested1_mixed.dirscan'}}
    outputs = {files: {:analysis => 'tmp/nested1_mixed.dirscan.analysis'}}
    ds = DirScanWorker.new(inputs, outputs)
    analysis_result = ds.analyze
    assert_equal({:file_sizes=>{0=>4, 5=>2, 12=>3, 50=>3}, :dir_sizes=>{0=>1, 5=>2, 62=>3, 196=>1}}, analysis_result)

    # analysis report
    inputs = {files: {:analysis => 'tmp/nested1_mixed.dirscan.analysis'}}
    outputs = {files: {:analysis_report => 'tmp/nested1_mixed.dirscan.analysis.report'}}
    ds = DirScanWorker.new(inputs, outputs)
    analysis_report = ds.analysis_report
    assert_equal({:sorted_by_count=>[[0, 4], [12, 3], [50, 3], [5, 2]]}, analysis_report)

    # iddupe_files - uses analysis to find exact duplicate files
    inputs = {
      files: {
        scan_index: 'tmp/nested1_mixed.dirscan',
        analysis: 'tmp/nested1_mixed.dirscan.analysis'
      }
    }
    outputs = {
      files: {
        iddupe_files: 'tmp/nested1_mixed.dirscan.analysis.iddupe_files',
        sha256_cache: 'tmp/nested1_mixed.dirscan.sha256_cache',
      }
    }
    ds = DirScanWorker.new(inputs, outputs)
    iddupe_files_result = ds.iddupe_files
    assert_equal(
      {collection_by_file_size: {
        12 =>
          {"a948904f2f0f479b8f8197694b30184b0d2ed1c1cd2a1ec0fb85d299a192a447"=>
            ["test_data/nested1_mixed/dir1/file1",
             "test_data/nested1_mixed/dir2/file1"]
          },
        50 =>
          {"9b8728603c656ce16230326e3ca3849e963e1fd13b75f1fede3334eec1568df5"=>
            ["test_data/nested1_mixed/dir1/story",
             "test_data/nested1_mixed/dir2/story",
             "test_data/nested1_mixed/dir3/story"]
          }
        },
      },
      iddupe_files_result,
    )
    assert_json_file_contains(
      {
        sha256_by_path: {
          "test_data/nested1_mixed/dir1/file1" => "a948904f2f0f479b8f8197694b30184b0d2ed1c1cd2a1ec0fb85d299a192a447",
          "test_data/nested1_mixed/dir1/story" => "9b8728603c656ce16230326e3ca3849e963e1fd13b75f1fede3334eec1568df5",
          "test_data/nested1_mixed/dir2/file1" => "a948904f2f0f479b8f8197694b30184b0d2ed1c1cd2a1ec0fb85d299a192a447",
          "test_data/nested1_mixed/dir2/story" => "9b8728603c656ce16230326e3ca3849e963e1fd13b75f1fede3334eec1568df5",
          "test_data/nested1_mixed/dir3/file3" => "d9a4c6676a62cb3b8ca0b8459ab341837cdba8543316c8574b454ccc24d4c690",
          "test_data/nested1_mixed/dir3/story" => "9b8728603c656ce16230326e3ca3849e963e1fd13b75f1fede3334eec1568df5",
          "test_data/nested1_mixed/dir4/file4" => "ab929fcd5594037960792ea0b98caf5fdaf6b60645e4ef248c28db74260f393e",
          "test_data/nested1_mixed/dir5/file5" => "ac169f9fb7cb48d431466d7b3bf2dc3e1d2e7ad6630f6b767a1ac1801c496b35",
        },
      },
      'tmp/nested1_mixed.dirscan.sha256_cache'
    )

    # iddupe_files report - creates a summary from the iddupe_files result, showing the number of redundant bytes for each file-size
    inputs = {files: {:iddupe_files => 'tmp/nested1_mixed.dirscan.analysis.iddupe_files'}}
    outputs = {files: {:iddupe_files_report => 'tmp/nested1_mixed.dirscan.analysis.iddupe_files.json'}}
    ds = DirScanWorker.new(inputs, outputs)
    iddupe_files_report = ds.iddupe_files_report
    assert_equal(
      {
        :dupes_by_file_size=>[
          [50, 100, {"9b8728603c656ce16230326e3ca3849e963e1fd13b75f1fede3334eec1568df5" => ["test_data/nested1_mixed/dir1/story", "test_data/nested1_mixed/dir2/story", "test_data/nested1_mixed/dir3/story"]}],
          [12, 12, {"a948904f2f0f479b8f8197694b30184b0d2ed1c1cd2a1ec0fb85d299a192a447" => ["test_data/nested1_mixed/dir1/file1", "test_data/nested1_mixed/dir2/file1"]}]
        ],
        :summary=>{:total_redundant_files_count=>3, :total_redundant_size=>112}
      },
      iddupe_files_report
    )

    # iddupe_dirs - uses iddupe_files to find duplicate dir's
    inputs = {
      files: {
        :scan_index => 'tmp/nested1_mixed.dirscan',
        :analysis => 'tmp/nested1_mixed.dirscan.analysis',
        :iddupe_files => 'tmp/nested1_mixed.dirscan.analysis.iddupe_files',
        :sha256_cache => 'tmp/nested1_mixed.dirscan.sha256_cache',
      }
    }
    outputs = {
      files: {
        :iddupe_dirs => 'tmp/nested1_mixed.dirscan.analysis.iddupe_dirs'
      }
    }
    ds = DirScanWorker.new(inputs, outputs)
    result = ds.iddupe_dirs
    assert_equal(
      {
        :collection_by_dir_size=>{
          62=>{
            "2e5fbfb3a248c9fa7575c3dd1301ec18" => ["test_data/nested1_mixed/dir1", "test_data/nested1_mixed/dir2"]
          }
        }
      },
      result,
    )

  end
   
end
