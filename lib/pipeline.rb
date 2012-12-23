class Object
  def blank?
    respond_to?(:empty?) ? empty? : !self
  end
end

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

  def output_value(sym, options = {})
    if outputs[:values] && outputs[:values].key?(sym)
      return outputs[:values][sym]
    else
      return options[:default] if options.key?(:default)
      raise ArgumentError, "No value for outputs[:values][:#{sym}]"
    end
  end

  def input_file(sym, options = {})
    if inputs[:files] && inputs[:files].key?(sym)
      return inputs[:files][sym]
    else
      return options[:default] if options.key?(:default)
      raise ArgumentError, "No value for inputs[:files][:#{sym}]"
    end
  end

  def output_file(sym, options = {})
    if outputs[:files] && outputs[:files].key?(sym)
      return outputs[:files][sym]
    else
      return options[:default] if options.key?(:default)
      raise ArgumentError, "No value for outputs[:files][:#{sym}]"
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


class LazyJob < Job # will not run the job if the output file(s) already exist
  def run
    outputs[:files].each do |output_key, output_value|
      unless File.exist?(output_value)
        return super
      end
    end
    return nil
  end

  def simulate
    outputs[:files].each do |output_key, output_value|
      unless File.exist?(output_value)
        return super
      end
    end
    return nil
  end
end


# class DependencyJob < Job # will not run the job the output file(s) are newer than the input file(s)
#   def run
#     outputs.each do |output_key, output_value|
#       unless File.exist?(output_value)
#         return super
#       end
#     end
#     return nil
#   end

#   def simulate
#     outputs.each do |output_key, output_value|
#       unless File.exist?(output_value)
#         return super
#       end
#     end
#     return nil
#   end
# end


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