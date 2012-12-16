class Worker
  def initialize(inputs, outputs)
    @inputs = inputs
    @outputs = outputs
  end

  def required_inputs(*req_inputs)
    req_inputs.each do |req_input|
      unless @inputs.key?(req_input)
        throw "Missing input '#{req_input}'"
      end
    end
  end

  def required_outputs(*req_outputs)
    req_outputs.each do |req_output|
      unless @outputs.key?(req_output)
        throw "Missing input '#{req_output}'"
      end
    end
  end

  def input(sym, options = {})
    if @inputs.key?(sym)
      return @inputs[sym]
    else
      return options[:default] if options.key?(:default)
      throw "No input '#{sym}'"
    end
  end

  def output(sym, options = {})
    if @outputs.key?(sym)
      return @outputs[sym]
    else
      return options[:default] if options.key?(:default)
      throw "No output '#{sym}'"
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

  def run(job_class = Job)
    result = nil
    @config[:job_order].each do |job|
      job = job_class.new(@config[:jobs][job])
      result = job.run
    end
    return result
  end

  def simulate(job_class = Job)
    result = []
    @config[:job_order].each do |job|
      job = job_class.new(@config[:jobs][job])
      result << job.simulate
    end
    return result
  end
end