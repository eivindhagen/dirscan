class Worker
  def initialize(inputs, outputs)
    @inputs = inputs
    @outputs = outputs
  end

  def required_inputs(*req_inputs)
    req_inputs.each do |req_input|
      unless @inputs.key?(req_input)
        raise ArgumentError, "Required input :#{req_input_file} is missing from @inputs"
      end
    end
  end

  def required_outputs(*req_outputs)
    req_outputs.each do |req_output|
      unless @outputs.key?(req_output)
        raise ArgumentError, "Required output :#{req_output_file} is missing from @outputs"
      end
    end
  end

  def required_input_files(*req_input_files)
    req_input_files.each do |req_input_file|
      unless @inputs.key?(req_input_file)
        raise ArgumentError, "Required input file :#{req_input_file} is missing from @inputs"
      end
      # input files should also exist
      req_input_file_path = @inputs[req_input_file]
      unless File.exist?(req_input_file_path)
        raise ArgumentError, "Required input file :#{req_input_file} refers to path '#{req_input_file_path}' that does not exist"
      end
    end
  end

  def required_output_files(*req_output_files)
    req_output_files.each do |req_output_file|
      unless @outputs.key?(req_output_file)
        raise ArgumentError, "Required output file :#{req_output_file} is missing from @outputs"
      end
    end
  end

  def input(sym, options = {})
    if @inputs.key?(sym)
      return @inputs[sym]
    else
      return options[:default] if options.key?(:default)
      raise ArgumentError, "No input :#{sym}"
    end
  end

  def output(sym, options = {})
    if @outputs.key?(sym)
      return @outputs[sym]
    else
      return options[:default] if options.key?(:default)
      raise ArgumentError, "No output :#{sym}"
    end
  end
end


class Job
  def initialize(job_config)
    @config = job_config
  end

  def inputs
    @config[:inputs]
  end

  def outputs
    @config[:outputs]
  end

  def ruby_class
    Kernel.const_get(@config[:worker][:ruby_class].to_s)  # if this was Rails we could have done .to_s.constantize
  end

  def ruby_method
    @config[:worker][:ruby_method]
  end

  def run
    # create instance of the worker class, with the given inputs & outputs
    obj = ruby_class.new(inputs, outputs)
    # call the worker method
    obj.send ruby_method
  end

  def simulate
    "#{ruby_class}.new(#{inputs}, #{outputs}).#{ruby_method}"
  end
end


class LazyJob < Job # will not run the job if the output(s) already exist
  def run
    outputs.each do |output_key, output_value|
      unless File.exist?(output_value)
        return super
      end
    end
    return nil
  end

  def simulate
    outputs.each do |output_key, output_value|
      unless File.exist?(output_value)
        return super
      end
    end
    return nil
  end
end


class Pipeline
  def initialize(pipeline_config)
    @config = pipeline_config
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
      result = job.run
    end
    return result
  end

  def simulate(job_class = Job)
    result = []
    @config[:job_order].each do |job|
      job = job_class.new(config_for_job(job))
      result << job.simulate
    end
    return result
  end
end