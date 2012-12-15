# dirscan.rb

require File.join(File.dirname(__FILE__), 'lib', 'dirscanner')
require File.join(File.dirname(__FILE__), 'lib', 'pipeline')

def create_pipeline(scan_root, dst_files_dir)
  dirname = File.dirname(scan_root)

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
          report: analysis_report,
        },
        worker: {
          ruby_class: :DirScanner,
          ruby_method: :analyze,
        }
      },

      iddupe: { # positively identify duplicate files (within each group of same-size files)
        inputs: {
          analysis: analysis,
        },
        outputs: {
          report: iddupe_report,
        },
        worker: {
          ruby_class: :DirScanner,
          ruby_method: :iddupe,
        }
      },
    },

    # job_order: [:scan, :analyze, :iddupe],
    job_order: [:scan],
  }

  return Pipeline.new(pipeline_config)
end

# command line arguments
case ARGV[0]
when 's'
  # scan dir, creates an index file
  scan_root = ARGV[1]
  index_path = ARGV[2]

  ds = DirScanner.new(
    :scan_root => scan_root,
    :index_path => index_path,
  )
  ds.scan

when 'sq'
  # scan dir quickly, creates an index file
  scan_root = ARGV[1]
  index_path = ARGV[2]

  ds = DirScanner.new(
    :scan_root => scan_root,
    :index_path => index_path,
    :quick_scan => true,
  )
  ds.scan

when 'v'
  # verify index file, compares with original dir
  index_path = ARGV[1]

  ds = DirScanner.new(
    :index_path => index_path,
  )
  ds.verify

when 'u'
  # unpack index file, writes a text version of the file
  index_path = ARGV[1]
  output_path = ARGV[2]

  ds = DirScanner.new(
    :index_path => index_path,
  )
  ds.unpack(output_path)

when 'a'
  # analyze index file, writes a JSON analysis document to the output file
  index_path = ARGV[1]
  output_path = ARGV[2]

  ds = DirScanner.new(
    :index_path => index_path,
  )
  ds.analyze(output_path)

when 'ar'
  # analysis report, reads the JSON analysis document from the output file
  index_path = ARGV[1]
  output_path = ARGV[2]

  ds = DirScanner.new(
    :analysis_path => index_path,
  )
  ds.analyze_report(output_path) 

when 'p'
  # run a pipeline of jobs
  scan_root = ARGV[1]
  output_files_dir = ARGV[2]

  pipeline = create_pipeline(scan_root, output_files_dir)
  puts pipeline.simulate
end

