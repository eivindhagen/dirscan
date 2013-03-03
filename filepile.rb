# filepile.rb

require File.join(File.dirname(__FILE__), 'lib', 'hostinfo')
require File.join(File.dirname(__FILE__), 'lib', 'file_job')
require File.join(File.dirname(__FILE__), 'lib', 'indexfile_job')
require File.join(File.dirname(__FILE__), 'lib', 'dirscan_job')
require File.join(File.dirname(__FILE__), 'lib', 'file_pile_job')
require File.join(File.dirname(__FILE__), 'lib', 'pipeline')
require File.join(File.dirname(__FILE__), 'lib', 'file_pile_dir')
require File.expand_path('lib/logging', File.dirname(__FILE__))

class Command
  # Mix in the ability to log stuff ...
  include Logging

  def create_pipeline_for_sha256(path)
    pipeline_config = {
      jobs: {
        sha256: { # read input path, generate output value
          inputs: {
            files: {
              path: path,
            }
          },
          outputs: {
            values: {
              sha256: nil,
            },
          },
          job: {
            ruby_class: :FileJob,
            ruby_method: :sha256,
          }
        }

      },

      job_order: [:sha256],
    }

    return Pipeline.new(pipeline_config)
  end

  def create_pipeline_for_copy_file(src_path, dst_path)
    pipeline_config = {
      jobs: {
        copy_file: { # read 2 input sqlite3 files, create a single sqlite3 output file
          inputs: {
            files: {
              src_path: src_path,
            }
          },
          outputs: {
            files: {
              dst_path: dst_path,
            },
          },
          job: {
            ruby_class: :FileJob,
            ruby_method: :copy,
          }
        }

      },

      job_order: [:copy_file],
    }

    return Pipeline.new(pipeline_config)
  end

  def create_pipeline_for_move_file(src_path, dst_path)
    pipeline_config = {
      jobs: {
        move_file: { # read 2 input sqlite3 files, create a single sqlite3 output file
          inputs: {
            files: {
              src_path: src_path,
            }
          },
          outputs: {
            files: {
              dst_path: dst_path,
            },
          },
          job: {
            ruby_class: :FileJob,
            ruby_method: :move,
          }
        }

      },

      job_order: [:move_file],
    }

    return Pipeline.new(pipeline_config)
  end

  def create_pipeline_for_delete_file(dst_path)
    pipeline_config = {
      jobs: {
        delete_file: { # read 2 input sqlite3 files, create a single sqlite3 output file
          outputs: {
            files: {
              dst_path: dst_path,
            },
          },
          job: {
            ruby_class: :FileJob,
            ruby_method: :delete,
          }
        }

      },

      job_order: [:delete_file],
    }

    return Pipeline.new(pipeline_config)
  end

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
          job: {
            ruby_class: :FilePileJob,
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
          job: {
            ruby_class: :IndexFileJob,
            ruby_method: :unpack,
          }
        }

      },

      job_order: [:store, :unpack],
    }

    return Pipeline.new(pipeline_config)
  end

  def create_pipeline_for_store_update(scan_root, filepile_root)
    filepile = FilePileDir.new(filepile_root)

    timestamp = Time.now.to_i
    checksum = StringHash.md5(HostInfo.name + scan_root + filepile_root) # make it unique to avoid file name collisions
    scan_index = File.join filepile.logs, "#{timestamp}_#{checksum}.storeupdate"
    scan_unpack = scan_index + '.json'

    pipeline_config = {
      jobs: {
        store_update: { # scan a directory and store each file in the filepile system, but ignore any file that is already in the filepile
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
          job: {
            ruby_class: :FilePileJob,
            ruby_method: :store_update,
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
          job: {
            ruby_class: :IndexFileJob,
            ruby_method: :unpack,
          }
        }

      },

      job_order: [:store_update, :unpack],
    }

    return Pipeline.new(pipeline_config)
  end

  def create_pipeline_for_unpack(scan_index, scan_unpack)
    pipeline_config = {
      jobs: {
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
          job: {
            ruby_class: :IndexFileJob,
            ruby_method: :unpack,
          }
        }

      },

      job_order: [:unpack],
    }

    return Pipeline.new(pipeline_config)
  end

  def create_pipeline_for_scan(scan_root, dst_files_dir)
    dirname = File.basename(scan_root)

    scan_index = File.join dst_files_dir, "#{dirname}.scan"
    sha256_cache = "#{scan_index}.sha256_cache"
    scan_unpack = "#{scan_index}.unpack"
    analysis = "#{scan_index}.analysis"
    analysis_report = "#{analysis}.report"
    iddupe_files = "#{analysis_report}.iddupe_files"
    iddupe_files_report = "#{iddupe_files}.report"

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
          job: {
            ruby_class: :DirScanJob,
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
          job: {
            ruby_class: :IndexFileJob,
            ruby_method: :unpack,
          }
        },

        analysis: { # analysis of the directory scan to identify which file sizes may contain duplicates (same-size is step 1 in finding dupes)
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
          job: {
            ruby_class: :DirScanJob,
            ruby_method: :analyze,
          }
        },

        analysis_report: { # generate report from analyze
          inputs: {
            files: {
              analysis: analysis,
            },
          },
          outputs: {
            files: {
              analysis_report: analysis_report,
            },
          },
          job: {
            ruby_class: :DirScanJob,
            ruby_method: :analysis_report,
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
          job: {
            ruby_class: :DirScanJob,
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
          job: {
            ruby_class: :DirScanJob,
            ruby_method: :iddupe_files_report,
          }
        },
      },

      job_order: [:scan, :unpack, :analysis, :analysis_report, :iddupe_files, :iddupe_files_report],
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
          job: {
            ruby_class: :IndexFileJob,
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
          job: {
            ruby_class: :IndexFileJob,
            ruby_method: :export_sqlite3,
          }
        }

      },

      job_order: [:export_sqlite3],
    }

    return Pipeline.new(pipeline_config)
  end


  def create_pipeline_for_create_sqlite3(db_path)
    pipeline_config = {
      jobs: {
        create_sqlite3: { # create an empty sqlite3 file
          outputs: {
            files: {
              db_path: db_path,
            },
          },
          job: {
            ruby_class: :IndexFileJob,
            ruby_method: :create_sqlite3,
          }
        }

      },

      job_order: [:create_sqlite3],
    }

    return Pipeline.new(pipeline_config)
  end


  def create_pipeline_for_diff_sqlite3(diff_operation, input1_path, input2_path, output_path)
    pipeline_config = {
      jobs: {
        diff_sqlite3: { # read 2 input sqlite3 files, output subset of input1 that is either unique or in common with input2 (selected by diff_operation)
          inputs: {
            values:{
              diff_operation: diff_operation,
            },
            files: {
              db_in1_path: input1_path,
              db_in2_path: input2_path,
            },
          },
          outputs: {
            files: {
              db_out_path: output_path,
            },
          },
          job: {
            ruby_class: :IndexFileJob,
            ruby_method: :diff_sqlite3,
          },
        }

      },

      job_order: [:diff_sqlite3],
    }

    return Pipeline.new(pipeline_config)
  end


  def create_pipeline_for_merge_sqlite3(input1_path, input2_path, output_path)
    pipeline_config = {
      jobs: {
        merge_sqlite3: { # read 2 input sqlite3 files, create a single sqlite3 output file
          inputs: {
            files: {
              db_in1_path: input1_path,
              db_in2_path: input2_path,
            }
          },
          outputs: {
            files: {
              db_out_path: output_path,
            },
          },
          job: {
            ruby_class: :IndexFileJob,
            ruby_method: :merge_sqlite3,
          }
        }

      },

      job_order: [:merge_sqlite3],
    }

    return Pipeline.new(pipeline_config)
  end


  def create_pipeline_for_inspect_sqlite3(db_path)
    pipeline_config = {
      jobs: {
        inspect_sqlite3: { # inspect a sqlite3 database
          inputs: {
            files: {
              db_path: db_path,
            }
          },
          outputs: {
            values: {
              files_count: nil,
            },
          },
          job: {
            ruby_class: :IndexFileJob,
            ruby_method: :inspect_sqlite3,
          }
        }

      },

      job_order: [:inspect_sqlite3],
    }

    return Pipeline.new(pipeline_config)
  end


  def process_cmdline(args)
    # command line arguments
    command = args[0]
    case command

    when 'sha256'
      # calculate sha256 for a file
      path = args[1].split('\\').join('/')
      pipeline = create_pipeline_for_sha256(path)
      pipeline.run(Worker) # Use the 'Worker' class to redo the work no matter what
      result = pipeline.config_for_job(:sha256).to_yaml
      logger.info result

    when 'store'
      # store files in a filepile, by recursive scan of input folder
      
      # arg 1 is the location of the input files (should be a directory, will be scanned recursively)
      scan_root = args[1].split('\\').join('/')
      # arg 2 is the location of the output filepile (it's root path, will be created if it does not exist)
      filepile_root = args[2].split('\\').join('/')

      logger.info "storing dir '#{scan_root}' to FilePile '#{filepile_root}'"

      pipeline = create_pipeline_for_store(scan_root, filepile_root)
      pipeline.add_options(debug_level: :all)
      pipeline.run(Worker) # Use the 'Worker' class to make it run even if the output folder exist

    when 'store_update'
      # store new & modified files in a filepile, by recursive scan of input folder
      # this is faster than storing everything, as it avoids generating metadata for files that are already in the filepile
      
      # arg 1 is the location of the input files (should be a directory, will be scanned recursively)
      scan_root = args[1].split('\\').join('/')
      # arg 2 is the location of the output filepile (it's root path, will be created if it does not exist)
      filepile_root = args[2].split('\\').join('/')

      pipeline = create_pipeline_for_store_update(scan_root, filepile_root)
      pipeline.add_options(debug_level: :all)
      pipeline.run(Worker) # Use the 'Worker' class to make it run even if the output folder exist

    when 'unpack'
      # export CSV file from an index file
      scan_index = args[1].split('\\').join('/')
      scan_unpack = args[2].split('\\').join('/')

      pipeline = create_pipeline_for_unpack(scan_index, scan_unpack)
      pipeline.run(Worker) # Use the 'Worker' class to redo the work no matter what

    when 'unpack_all'
      # unoack all .store files in the filepile's log directory
      filepile_root = args[1].split('\\').join('/')

      filepile = FilePileDir.new(filepile_root)

      # process all *.store files
      Dir[File.join(filepile.logs, '*.store')].sort.each do |full_path|
        begin
          unpack_path = full_path + '.json'
          logger.debug "Unpacking '#{full_path}' to '#{unpack_path}'"
          pipeline = create_pipeline_for_unpack(full_path, unpack_path)
          pipeline.run(DependencyWorker) # Use the 'DependencyWorker' class to skip re-creating output if input is older
        rescue Exception => e
          logger.error "Exception while unpacking '#{full_path}' to '#{unpack_path}'"
          logger.error e.message
          logger.error e.backtrace
        end
      end

    when 'scan'
      # scan a directory and generate scan_index, analysis, and reports
      scan_root = args[1].split('\\').join('/')
      output_files_dir = args[2].split('\\').join('/')

      pipeline = create_pipeline_for_scan(scan_root, output_files_dir)
      # pipeline.simulate(LazyWorker) # Use the 'LazyWorker' class to make it run only if the output does not already exist
      pipeline.run(DependencyWorker) # Use the 'LazyWorker' class to make it run only if the output does not already exist

    when 'export_csv'
      # export CSV file from an index file
      index_path = args[1].split('\\').join('/')
      csv_path = args[2].split('\\').join('/')

      pipeline = create_pipeline_for_export_csv(index_path, csv_path)
      # pipeline.run(DependencyWorker) # Use the 'DependencyWorker' class to skip re-creating output if input is older
      pipeline.run(Worker) # Use the 'Worker' class to redo the work no matter what

    when 'export_csv_all'
      # export csv file for all index files in the filepile's log directory
      filepile_root = args[1].split('\\').join('/')

      filepile = FilePileDir.new(filepile_root)

      # process all *.store files
      Dir[File.join(filepile.logs, '*.store')].sort.each do |full_path|
        begin
          csv_path = full_path + '.csv'
          logger.debug "Exporting '#{full_path}' to '#{csv_path}'"
          pipeline = create_pipeline_for_export_csv(full_path, csv_path)
          pipeline.run(DependencyWorker) # Use the 'DependencyWorker' class to skip re-creating output if input is older
        rescue Exception => e
          logger.error "Exception while exporting '#{full_path}' to '#{csv_path}'"
          logger.error e.message
          logger.error e.backtrace
        end
      end

    when 'export_sqlite3'
      # export sqlite3 database from an index file
      index_path = args[1].split('\\').join('/')
      db_path = args[2].split('\\').join('/')

      pipeline = create_pipeline_for_export_sqlite3(index_path, db_path)
      # pipeline.run(DependencyWorker) # Use the 'DependencyWorker' class to skip re-creating output if input is older
      pipeline.run(Worker) # Use the 'Worker' class to redo the work no matter what

    when 'export_sqlite3_all'
      # export sqlite3 database for all index files in the filepile's log directory
      filepile_root = args[1].split('\\').join('/')

      filepile = FilePileDir.new(filepile_root)

      # process all *.store files
      Dir[File.join(filepile.logs, '*.store')].sort.each do |full_path|
        begin
          sqlite3_path = full_path + '.sqlite3'
          logger.debug "Exporting '#{full_path}' to '#{sqlite3_path}'"
          pipeline = create_pipeline_for_export_sqlite3(full_path, sqlite3_path)
          pipeline.run(DependencyWorker) # Use the 'DependencyWorker' class to skip re-creating output if input is older
        rescue Exception => e
          logger.error "Exception while exporting '#{full_path}' to '#{sqlite3_path}'"
          logger.error e.message
          logger.error e.backtrace
        end
      end

    when 'diff_sqlite3'
      # diff two sqlite3 files, output the subset of records from input1 that is either unique or in common with input2 (as selected by diff_operation)
      diff_operation = args[1] # 'unique' or 'common'
      input1_path = args[2].split('\\').join('/')
      input2_path = args[3].split('\\').join('/')
      output_path = args[4].split('\\').join('/')

      pipeline = create_pipeline_for_diff_sqlite3(diff_operation, input1_path, input2_path, output_path)
      pipeline.run(Worker) # Use the 'Worker' class to redo the work no matter what

    when 'merge_sqlite3'
      # merge two sqlite3 files, output a third file
      input1_path = args[1].split('\\').join('/')
      input2_path = args[2].split('\\').join('/')
      output_path = args[3].split('\\').join('/')

      pipeline = create_pipeline_for_merge_sqlite3(input1_path, input2_path, output_path)
      pipeline.run(Worker) # Use the 'Worker' class to redo the work no matter what

    when 'merge_sqlite3_all'
      # merge all sqlite3 databases (logs/*.sqlite3) into a single database (metadata/db.sqlite3)
      filepile_root = args[1].split('\\').join('/')

      filepile = FilePileDir.new(filepile_root)
      final_db_path = File.join filepile.metadata, 'db.sqlite3'
      merge_db_path = File.join filepile.temp, 'db_merge.sqlite3' # TODO: avoid collisions here

      # create empty final db if none exist already
      unless File.exist? final_db_path
        pipeline = create_pipeline_for_create_sqlite3(final_db_path)
        pipeline.run(Worker)
      end

      # process all *.store files, merge each one into the final db
      Dir[File.join(filepile.logs, '*.sqlite3')].sort.each do |full_path|
        # merge into new file (temp)
        logger.debug "merge into new file (temp)"
        pipeline = create_pipeline_for_merge_sqlite3(final_db_path, full_path, merge_db_path)
        pipeline.run(Worker)

        # delete old version of final
        logger.debug "delete old version of final"
        pipeline = create_pipeline_for_delete_file(final_db_path)
        pipeline.run(Worker)

        # move merged (temp) to final
        logger.debug "move merged (temp) to final"
        pipeline = create_pipeline_for_move_file(merge_db_path, final_db_path)
        pipeline.run(Worker)
      end

    when 'inspect_sqlite3'
      # inspect a sqlite3 database
      db_path = args[1].split('\\').join('/')

      pipeline = create_pipeline_for_inspect_sqlite3(db_path)
      pipeline.run(Worker) # Use the 'Worker' class to redo the work no matter what

    end
  end

  def log_level_from_string(level)
    case level
    when 'fatal' then Logger::FATAL
    when 'error' then Logger::ERROR
    when 'warn' then Logger::WARN
    when 'info' then Logger::INFO
    when 'debug' then Logger::DEBUG
    end
  end

  def execute(args)
    log_level = Logger::INFO

    trimmed_args = []

    state = :none
    args.each_with_index do |arg, arg_index|
      case arg
      when '-log'
        log_level = log_level_from_string(args[arg_index + 1])
        args.delete_at(arg_index + 1)
        next
      end

      trimmed_args.push arg
    end

    Logging.set_default_log_level(log_level)

    start_time = Time.now

    process_cmdline(trimmed_args)

    end_time = Time.now
    delta_time = end_time - start_time
    logger.info "Total time: #{delta_time}"
  end
end


command = Command.new
command.execute(ARGV)
