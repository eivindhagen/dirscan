# filepile.rb

require File.join(File.dirname(__FILE__), 'lib', 'hostinfo')
require File.join(File.dirname(__FILE__), 'lib', 'indexfile_worker')
require File.join(File.dirname(__FILE__), 'lib', 'dirscan_worker')
require File.join(File.dirname(__FILE__), 'lib', 'file_pile_worker')
require File.join(File.dirname(__FILE__), 'lib', 'pipeline')
require File.join(File.dirname(__FILE__), 'lib', 'file_pile_dir')

def create_pipeline_for_store(scan_root, filepile_root)
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
          ruby_class: :FilePileWorker,
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

def create_pipeline_for_scan(scan_root, dst_files_dir)
  dirname = File.basename(scan_root)

  scan_index = File.join dst_files_dir, "#{dirname}.dirscan"
  scan_unpack = File.join dst_files_dir, "#{dirname}.dirscan.unpack"
  analysis = File.join dst_files_dir, "#{dirname}.dirscan.analysis"
  analysis_report = File.join dst_files_dir, "#{dirname}.dirscan.analysis.report"
  iddupe_files = File.join dst_files_dir, "#{dirname}.dirscan.analysis.iddupe_files"
  sha256_cache = File.join dst_files_dir, "#{dirname}.dirscan.sha256_cache"
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
          ruby_class: :DirScanWorker,
          ruby_method: :scan,
        }
      },

      unpack: { # unpack a scan_index into a human readable format (JSON)
        inputs: {
          files: {
            scan_index: scan_index,
          },
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
          ruby_class: :DirScanWorker,
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
            sha256_cache: sha256_cache,
          },
        },
        worker: {
          ruby_class: :DirScanWorker,
          ruby_method: :iddupe_files,
        }
      },

      iddupe_files_report: { # generate summary of duplicate files, including the number of redundant bytes or each file-size
        inputs: {
          files: {
            iddupe_files: iddupe_files,
            sha256_cache: sha256_cache,
          },
        },
        outputs: {
          files: {
            iddupe_files_report: iddupe_files_report,
          },
        },
        worker: {
          ruby_class: :DirScanWorker,
          ruby_method: :iddupe_files_report,
        }
      },
    },

    job_order: [:scan, :unpack, :analyze, :iddupe_files, :iddupe_files_report],
  }

  return Pipeline.new(pipeline_config)
end

def create_pipeline_for_export_csv(index_path, csv_path)
  pipeline_config = {
    jobs: {
      export_csv: { # read the index file, write a CSV file
        inputs: {
          files: {
            index_path: index_path,
          }
        },
        outputs: {
          files: {
            csv_path: csv_path,
          },
        },
        worker: {
          ruby_class: :IndexFileWorker,
          ruby_method: :export_csv,
        }
      }

    },

    job_order: [:export_csv],
  }

  return Pipeline.new(pipeline_config)
end


def create_pipeline_for_export_sqlite3(index_path, db_path)
  pipeline_config = {
    jobs: {
      export_sqlite3: { # read the index file, write a SQLite3 database file
        inputs: {
          files: {
            index_path: index_path,
          }
        },
        outputs: {
          files: {
            db_path: db_path,
          },
        },
        worker: {
          ruby_class: :IndexFileWorker,
          ruby_method: :export_sqlite3,
        }
      }

    },

    job_order: [:export_sqlite3],
  }

  return Pipeline.new(pipeline_config)
end


# command line arguments
command = ARGV[0]
case command

when 'store'
  # store files in a filepile
  
  # arg 1 is the location of the input files (should be a directory, will be scanned recursively)
  scan_root = ARGV[1].split('\\').join('/')
  # arg 2 is the location of the output filepile (it's root path, will be created if it does not exist)
  filepile_root = ARGV[2].split('\\').join('/')

  pipeline = create_pipeline_for_store(scan_root, filepile_root)
  pipeline.add_options(debug_level: :all)
  pipeline.run(Job) # Use the 'Job' class to make it run even if the output folder exist

when 'scan'
  # scan a directory and generate scan_index, analysis, and reports
  scan_root = ARGV[1].split('\\').join('/')
  output_files_dir = ARGV[2].split('\\').join('/')

  pipeline = create_pipeline_for_scan(scan_root, output_files_dir)
  # pipeline.simulate(LazyJob) # Use the 'LazyJob' class to make it run only if the output does not already exist
  pipeline.run(LazyJob) # Use the 'LazyJob' class to make it run only if the output does not already exist

when 'export_csv'

  # scan a directory and generate scan_index, analysis, and reports
  index_path = ARGV[1].split('\\').join('/')
  csv_path = ARGV[2].split('\\').join('/')

  pipeline = create_pipeline_for_export_csv(index_path, csv_path)
  # pipeline.run(DependencyJob) # Use the 'DependencyJob' class to skip re-creating the CSV file if it's newer than the index file
  pipeline.run(Job) # Use the 'Job' class to redo the work no matter what

when 'export_sqlite3'

  # scan a directory and generate scan_index, analysis, and reports
  index_path = ARGV[1].split('\\').join('/')
  db_path = ARGV[2].split('\\').join('/')

  pipeline = create_pipeline_for_export_sqlite3(index_path, db_path)
  # pipeline.run(DependencyJob) # Use the 'DependencyJob' class to skip re-creating the CSV file if it's newer than the index file
  pipeline.run(Job) # Use the 'Job' class to redo the work no matter what

end
