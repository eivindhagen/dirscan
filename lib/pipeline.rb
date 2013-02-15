# Pipeline system
#
# A Pipeline is made up of Jobs, where each job's work is performed by a method defined in a Worker class
#
# Worker   : Implements the actual code for doing a specific type of work, i.e. process input files into output files
#
# Job      : Determines if the work needs to be done, and will skip the work if it thinks it has been done already.
#            This is an optimization layer. Different types of Job classes will skip work using different criteria.
#
# Pipeline : Executes a chain of jobs, in a predetermined order. Uses Jobs to decide if the work needs to be done, or can be skipped.

require 'debugger'
require 'yaml'

require File.join(File.dirname(__FILE__), 'pathinfo')

# Rails adds the blank?() method to the Object class, and we want that here too (No Rails)
class Object
  def blank?
    respond_to?(:empty?) ? empty? : !self
  end
end

# Worker is a base class for implementing more specialized worker classes.
# A Worker class is executed by a Job, see the Job class for more info.
#
# By inheriting from the Worker class, your worker class have easy access to
# the input and output arguments, which are broken down into files & values
class Worker
  def initialize(inputs, outputs)
    @inputs = inputs || {}
    @outputs = outputs || {}

    unless @inputs.kind_of? Hash
      raise ArgumentError, "Required config inputs is missing"
    end
    unless @outputs.kind_of? Hash
      raise ArgumentError, "Required config outputs is missing"
    end
  end

  def inputs
    @inputs
  end

  def outputs
    @outputs
  end

  def required_input_values(*req_input_values)
    return if req_input_values.blank?

    unless inputs[:values].kind_of? Hash
      raise ArgumentError, "Required config inputs[:values] is missing"
    end

    req_input_values.each do |req_input_value|
      unless inputs[:values].key?(req_input_value)
        raise ArgumentError, "Required config inputs[:values][:#{req_input_file}] is missing"
      end
    end
  end

  def required_output_values(*req_output_values)
    return if req_output_values.blank?

    unless outputs[:values].kind_of? Hash
      raise ArgumentError, "Required config outputs[:values] is missing"
    end

    req_output_values.each do |req_output_value|
      unless outputs[:values].key?(req_output_value)
        raise ArgumentError, "Required config outputs[:values][:#{req_output_file}] is missing"
      end
    end
  end

  # verifies that the given input files/paths are specified in the config, and that the files/paths exist
  def required_input_files(*req_input_files)
    return if req_input_files.blank?

    unless inputs[:files].kind_of? Hash
      raise ArgumentError, "Required config inputs[:files] is missing"
    end

    req_input_files.each do |req_input_file|
      unless inputs[:files].key?(req_input_file)
        raise ArgumentError, "Required config input[:files][:#{req_input_file}] is missing"
      end
      # input files should also exist
      req_input_file_path = inputs[:files][req_input_file]
      unless File.exist?(req_input_file_path)
        raise ArgumentError, "Required config input[:files][:#{req_input_file}] refers to path '#{req_input_file_path}' that does not exist"
      end
    end
  end

  # verifies that the given output files/paths are specific in the config
  def required_output_files(*req_output_files)
    return if req_output_files.blank?

    unless outputs[:files].kind_of? Hash
      raise ArgumentError, "Required config outputs[:files] is missing"
    end

    req_output_files.each do |req_output_file|
      unless outputs[:files].key?(req_output_file)
        raise ArgumentError, "Required config output[:files][:#{req_output_file}] is missing"
      end
    end
  end

  def input_value(sym, options = {})
    if inputs[:values] && inputs[:values].key?(sym)
      return inputs[:values][sym]
    else
      return options[:default] if options.key?(:default)
      raise ArgumentError, "No value for inputs[:values][:#{sym}]"
    end
  end

  def output_value(sym, value)
    if outputs[:values] && outputs[:values].key?(sym)
      outputs[:values][sym] = value
    else
      raise ArgumentError, "The output_value '#{sym}' was not found"
    end
  end

  # get the value (path) of an input-file parameter
  # if the given parameter does not exist, then options[:default] is returned, if it is present
  def input_file(sym, options = {})
    if inputs[:files] && inputs[:files].key?(sym)
      return inputs[:files][sym]
    else
      return options[:default] if options.key?(:default)
      raise ArgumentError, "No value for inputs[:files][:#{sym}]"
    end
  end

  # get the value (path) of an output-file parameter
  # if the given parameter does not exist, then options[:default] is returned, if it is present
  def output_file(sym, options = {})
    if outputs[:files] && outputs[:files].key?(sym)
      return outputs[:files][sym]
    else
      return options[:default] if options.key?(:default)
      raise ArgumentError, "No value for outputs[:files][:#{sym}]"
    end
  end
end


# Job is a base class, more specific job classes inherit from this one.
#
# A Job holds the configuration needed to create a Worker that can run (or simulate)
# This plain Job class will perform the job even if all teh work turns out to be redundant,
# i.e. it's not very smart about skipping work that has already been done
class Job
  def initialize(job_config)
    @config = job_config
  end

  def config
    @config
  end
  
  def worker
    config[:worker]
  end

  def inputs
    config[:inputs]
  end
  def input_files
    config[:inputs][:files]
  end
  def input_values
    config[:inputs][:values]
  end

  def outputs
    config[:outputs]
  end
  def output_files
    config[:outputs][:files]
  end
  def output_values
    config[:outputs][:values]
  end

  def to_s
    "#{worker[:ruby_class].to_s}::#{worker[:ruby_method]}"
  end

  def ruby_class
    Kernel.const_get(worker[:ruby_class].to_s)  # if this was Rails we could have done .to_s.constantize
  end

  def ruby_method
    worker[:ruby_method]
  end

  def run(options = {})
    puts "running job: #{self}" if options[:debug_level] == :all
    # create instance of the worker class, with the given inputs & outputs
    obj = ruby_class.new(inputs, outputs)
    # call the worker method
    obj.send ruby_method, options
  end

  def simulate(options = {})
    puts "simulating job: #{self}" if options[:debug_level] == :all
    "#{ruby_class}.new(#{inputs}, #{outputs}).#{ruby_method}(#{options})"
  end
end

# LazyJob will skip the work if the output file(s) already exist
# This will refuse to do work if the input files are fresh and the output files are stale, so beware!
class LazyJob < Job
  def output_files_exist?
    outputs[:files].each do |file_key, file_path|
      unless file_path && File.exist?(file_path)
        return false
      end
    end
    return true
  end

  def output_stale?
    return true unless output_files_exist?
  end

  def run(options)
    if output_stale?
      puts "performing job #{self} because output is stale" if options[:debug_level] == :all
      super(options)
    else
      puts "skipping job #{self} because output is fresh" if options[:debug_level] == :all
      nil
    end
  end

  def simulate(options)
    if output_stale?
      puts "performing job #{self} because output is stale" if options[:debug_level] == :all
      super(options)
    else
      puts "skipping job #{self} because output is fresh" if options[:debug_level] == :all
      nil
    end
  end
end

# DependencyJob will skip the work if the output file(s) are newer than the input file(s).
# It's bit smarter than the LazyJob class, since it WILL do the work if the output is stale.
class DependencyJob < LazyJob 
  def modify_times_for_files(files) # files = hash{name: path, ...}
    modify_times = {} # create a hash that mirrors the files hash (method argument)
    files.each do |file_key, file_path|
      modify_times[file_key] = PathInfo.new(file_path).modify_time
    end
    return modify_times
  end

  def output_stale?
    return true unless output_files_exist?

    input_modify_times = modify_times_for_files(input_files)
    output_modify_times = modify_times_for_files(output_files)
    puts "input files: #{input_modify_times.to_yaml}"
    puts "output files: #{output_modify_times.to_yaml}"
    input_modify_times.values.max >= output_modify_times.values.min # true if newest input file is newer than oldest output file
  end

  # run() is properly implemented in the super-class
  # simulate() is properly implemented in the super-class
end


# Pipeline can execute an entire job sequenze, where output from one job is used as input for other jobs
#
# The run() and simulate() methods create Job objects, which in turn execute Worker methods to do the work.
class Pipeline
  def initialize(pipeline_config, options = {})
    @config = pipeline_config
    @options = options
  end

  def options
    @options || {}
  end

  def add_options(options)
    @options = options.merge(options)
  end

  def config_for_job(job)
    unless @config[:jobs] && @config[:jobs][job]
      raise ArgumentError, "The job :#{job} was not found in @config[:jobs]"
    end
    @config[:jobs][job]
  end

  def run(job_class = Job)
    result = nil
    @config[:job_order].each do |job|
      job = job_class.new(config_for_job(job))
      result = job.run(options)
    end
    return result
  end

  def simulate(job_class = Job)
    result = []
    @config[:job_order].each do |job|
      job = job_class.new(config_for_job(job))
      result << job.simulate(options)
    end
    return result
  end
end