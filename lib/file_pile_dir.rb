# manages the directory structure for a File Pile
class FilePileDir
  private
  def dir_structure
    [
      {path: 'filedata', doc: 'file data, named by content sha256'},
      {path: 'metadata', doc: 'meta data, catalog information for each file in the pile'},
      {path: 'logs', doc: 'log files, generated by various file pile operations'},
      {path: 'temp', doc: 'temporary files, generated by various file pile operations and removed when no longer needed'},
    ]
  end

  # create object by specifying the location of the File Pile directory (even if it doesn't exist)
  # the necessary directories will be created if they do not already exist
  public
  def initialize(filepile_root)
    puts "FilePileDir::initialize(#{filepile_root})"
    @root = filepile_root

    # ensure the root folder exist, or create it
    unless Dir.exist?(@root)
      FileUtils.mkdir_p(@root)
      puts "root created: #{@root}"
    else
      puts "root exists: #{@root}"
    end

    # ensure each standard sub-dir exist, or create it
    dir_structure.each do |dir|
      fullpath = File.join @root, dir[:path]
      unless Dir.exist? fullpath
        FileUtils.mkdir_p fullpath
        puts "sub-dir created: #{fullpath}"
      else
        puts "sub-dir exists: #{fullpath}"
      end
    end
  end

  def filedata
    File.join @root, 'filedata'
  end

  def metadata
    File.join @root, 'metadata'
  end

  def logs
    File.join @root, 'logs'
  end

  def temp
    File.join @root, 'temp'
  end

end