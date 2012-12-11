# dirscan.rb

require File.join(File.dirname(__FILE__), 'lib', 'dirscanner')


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

end

