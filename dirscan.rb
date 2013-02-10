# dirscan.rb

require File.join(File.dirname(__FILE__), 'lib', 'hostinfo')
require File.join(File.dirname(__FILE__), 'lib', 'indexfile_worker')
require File.join(File.dirname(__FILE__), 'lib', 'dirscanner')
require File.join(File.dirname(__FILE__), 'lib', 'file_pile_storer')
require File.join(File.dirname(__FILE__), 'lib', 'pipeline')
require File.join(File.dirname(__FILE__), 'lib', 'file_pile_dir')

def create_pipeline(scan_root, dst_files_dir)
  dirname = File.basename(scan_root)

  scan_index = File.join dst_files_dir, "#{dirname}.dirscan"
  analysis = File.join dst_files_dir, "#{dirname}.dirscan.analysis"
  analysis_report = File.join dst_files_dir, "#{dirname}.dirscan.analysis.report"
  iddupe_files = File.join dst_files_dir, "#{dirname}.dirscan.analysis.iddupe_files"
  iddupe_files_report = File.join dst_files_dir, "#{dirname}.dirscan.analysis.iddupe_files.report"

  pipeline_config = {
    jobs: {
      scan: { # scan a directory and record file info for entire tree
        inputs: {
          files: {
            scan_root: scan_root,
          },
          values: {
            quick_scan: true,       # quick_scan = do not generate content checksums
          }
        },
        outputs: {
          files: {
            scan_index: scan_index,
          },
        },
        worker: {
          ruby_class: :DirScanner,
          ruby_method: :scan,
        }
      },

      analyze: { # analyze the directory scan to identify which file sizes may contain duplicates (same-size is step 1 in finding dupes)
        inputs: {
          files: {
            scan_index: scan_index,
          },
        },
        outputs: {
          files: {
            analysis: analysis,
          },
        },
        worker: {
          ruby_class: :DirScanner,
          ruby_method: :analyze,
        }
      },

      iddupe_files: { # positively identify duplicate files (within each group of same-size files)
        inputs: {
          files: {
            scan_index: scan_index,
            analysis: analysis,
          },
        },
        outputs: {
          files:{
            iddupe_files: iddupe_files,
          },
        },
        worker: {
          ruby_class: :DirScanner,
          ruby_method: :iddupe_files,
        }
      },

      iddupe_files_report: { # generate summary of duplicate files, including the number of redundant bytes or each file-size
        inputs: {
          files: {
            iddupe_files: iddupe_files,
          },
        },
        outputs: {
          files: {
            iddupe_files_report: iddupe_files_report,
          },
        },
        worker: {
          ruby_class: :DirScanner,
          ruby_method: :iddupe_files_report,
        }
      },
    },

    job_order: [:scan, :analyze, :iddupe_files, :iddupe_files_report],
  }

  return Pipeline.new(pipeline_config)
end

def create_pipeline_for_storage(scan_root, filepile_root)
  filepile = FilePileDir.new(filepile_root)

  timestamp = Time.now.to_i
  checksum = StringHash.md5(HostInfo.name + scan_root + filepile_root) # make it unique to avoid file name collisions
  scan_index = File.join filepile.logs, "#{timestamp}_#{checksum}.store"
  scan_unpack = scan_index + '.json'

  pipeline_config = {
    jobs: {
      store: { # scan a directory and store each file in the filepile system
        inputs: {
          files: {
            scan_root: scan_root,
          }
        },
        outputs: {
          files: {
            filepile_root: filepile_root,
            scan_index: scan_index,
          },
        },
        worker: {
          ruby_class: :FilePileStorer,
          ruby_method: :store,
        }
      },
      unpack: { # unpack the index file, writing a text version of the file
        inputs: {
          files: {
            scan_index: scan_index,
          }
        },
        outputs: {
          files: {
            scan_unpack: scan_unpack,
          },
        },
        worker: {
          ruby_class: :IndexFileWorker,
          ruby_method: :unpack,
        }
      }

    },

    job_order: [:store, :unpack],
  }

  return Pipeline.new(pipeline_config)
end

# command line arguments
command = ARGV[0]
case command

when 's'
  # store files in a filepile
  
  # arg 1 is the location of the input files (should be a directory, will be scanned recursively)
  scan_root = ARGV[1]
  # arg 2 is the location of the output filepile (it's root path, will be created if it does not exist)
  filepile_root = ARGV[2]

  pipeline = create_pipeline_for_storage(scan_root, filepile_root)
  pipeline.add_options(debug_level: :all)
  puts pipeline.run(Job) # Use the 'Job' class to make it run even if the output folder exist

when 'p'
  # run a pipeline of jobs
  scan_root = ARGV[1]
  output_files_dir = ARGV[2]

  pipeline = create_pipeline(scan_root, output_files_dir)
  puts pipeline.simulate(LazyJob) # Use the 'LazyJob' class to make it run only if the output does not already exist
  puts pipeline.run(LazyJob) # Use the 'LazyJob' class to make it run only if the output does not already exist

end
