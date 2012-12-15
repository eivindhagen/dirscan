class NaiveWorker
  def initialize(inputs, outputs)
    @inputs = inputs
    @outputs = outputs
  end
end

class Job
  def initialize(job_config)
    @config = job_config
  end

  def run
    ruby_class = @config[:worker][:ruby_class]
    ruby_method = @config[:worker][:ruby_method]
    inputs = @config[:inputs]
    outputs = @config[:outputs]

    # create instance of the worker class, with the given inputs & outputs
    obj = ruby_class.new(inputs, outputs)
    # call the worker method
    obj.send ruby_method
  end

  def simulate
    ruby_class = @config[:worker][:ruby_class]
    ruby_method = @config[:worker][:ruby_method]
    inputs = @config[:inputs]
    outputs = @config[:outputs]
    "#{ruby_class}.new(#{inputs}, #{outputs}).#{ruby_method}"
  end
end

class Pipeline
  def initialize(pipeline_config)
    @config = pipeline_config
  end

  def run
    result = nil
    @config[:job_order].each do |job|
      job = Job.new(@config[:jobs][job])
      result = job.run
    end
    return result
  end

  def simulate
    result = []
    @config[:job_order].each do |job|
      job = Job.new(@config[:jobs][job])
      result << job.simulate
    end
    return result
  end
end