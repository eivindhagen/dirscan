# dirscan.rb

require File.join(File.dirname(__FILE__), 'lib', 'dirscanner')
require File.join(File.dirname(__FILE__), 'lib', 'pipeline')

def create_pipeline(scan_root, dst_files_dir)
  dirname = File.basename(scan_root)

  scan_index = File.join dst_files_dir, "#{dirname}.dirscan"
  analysis = File.join dst_files_dir, "#{dirname}.dirscan.analysis"
  analysis_report = File.join dst_files_dir, "#{dirname}.dirscan.analysis.report"
  iddupe = File.join dst_files_dir, "#{dirname}.dirscan.analysis.iddupe"
  iddupe_report = File.join dst_files_dir, "#{dirname}.dirscan.analysis.iddupe.report"

  pipeline_config = {
    jobs: {
      scan: { # scan a directory and record file info for entire tree
        inputs: {
          scan_root: scan_root,
          quick_scan: true,       # quick_scan = do not generate content checksums
        },
        outputs: {
          scan_index: scan_index,
        },
        worker: {
          ruby_class: :DirScanner,
          ruby_method: :scan,
        }
      },

      analyze: { # analyze the directory scan to identify which file sizes may contain duplicates (same-size is step 1 in finding dupes)
        inputs: {
          scan_index: scan_index,
        },
        outputs: {
          analysis: analysis,
        },
        worker: {
          ruby_class: :DirScanner,
          ruby_method: :analyze,
        }
      },

      iddupe: { # positively identify duplicate files (within each group of same-size files)
        inputs: {
          scan_index: scan_index,
          analysis: analysis,
        },
        outputs: {
          iddupe: iddupe,
        },
        worker: {
          ruby_class: :DirScanner,
          ruby_method: :iddupe,
        }
      },

      iddupe_report: { # generate summary of duplicate files, including the number of redundant bytes or each file-size
        inputs: {
          iddupe: iddupe,
        },
        outputs: {
          iddupe_report: iddupe_report,
        },
        worker: {
          ruby_class: :DirScanner,
          ruby_method: :iddupe_report,
        }
      },
    },

    job_order: [:scan, :analyze, :iddupe, :iddupe_report],
  }

  return Pipeline.new(pipeline_config)
end

# command line arguments
command = ARGV[0]
case command

when 'p'
  # run a pipeline of jobs
  scan_root = ARGV[1]
  output_files_dir = ARGV[2]

  pipeline = create_pipeline(scan_root, output_files_dir)
  puts pipeline.simulate(LazyJob)
  puts pipeline.run(LazyJob)

end
