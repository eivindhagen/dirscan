require File.join(File.dirname(__FILE__), 'pathinfo')
require File.join(File.dirname(__FILE__), 'pipeline')
require File.join(File.dirname(__FILE__), 'hasher')

require 'pathname'
require 'fileutils'

class FileJob < Job
  attr_accessor :inputs, :outputs

  # calculate sah256 for a path
  def sha256(options = {})
    required_input_files :path
    required_output_values :sha256

    path = input_file(:path)

    unless File.exist?(path)
      raise "path '#{path}' does not exist"
    end
    
    sha256 = FileHash.sha256(path)
    output_value(:sha256, sha256)

    return true
  end

  # copy a file
  def copy(options = {})
    required_input_files :src_path
    required_output_files :dst_path

    src_path = input_file(:src_path)
    dst_path = output_file(:dst_path)

    if File.exist?(dst_path)
      raise "dst_path '#{dst_path}' already exist"
    end
    
    dst_dir = File.dirname(dst_path)
    FileUtils.mkdir_p(dst_dir) unless Dir.exist?(dst_dir)
    FileUtils.copy_file(src_path, dst_path)

    return true
  end

  # move a file (i.e. rename)
  def move(options = {})
    required_input_files :src_path
    required_output_files :dst_path

    src_path = input_file(:src_path)
    dst_path = output_file(:dst_path)

    if File.exist?(dst_path)
      raise "dst_path '#{dst_path}' already exist"
    end
    
    dst_dir = File.dirname(dst_path)
    FileUtils.mkdir_p(dst_dir) unless Dir.exist?(dst_dir)
    FileUtils.move(src_path, dst_path)

    return true
  end

  # delete a file
  def delete(options = {})
    required_output_files :dst_path

    dst_path = output_file(:dst_path)

    unless File.exist?(dst_path)
      raise "dst_path '#{dst_path}' does not exist"
    end
    FileUtils.rm_rf(dst_path)

    # make sure dst_path was destroyed
    if File.exist?(dst_path)
      raise "Unable to delete #{dst_path}, not sure why. Sorry..."
    end

    return true
  end
  
end
