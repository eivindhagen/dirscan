# unit_tests.rb
#
# http://en.wikibooks.org/wiki/Ruby_Programming/Unit_testing
#
require "test/unit"
require 'redgreen'

#include the classes we are testing
require File.expand_path('../lib/pipeline.rb', File.dirname(__FILE__))
require File.expand_path('../lib/assert_file_contains.rb', File.dirname(__FILE__))

PHASE_1_MSG = "This is phase 1 of the test."
PHASE_1_PATH = "tmp/pipeline_test_phase1.txt"

class MyWorker < Worker
  def generate
    required_input_files
    required_output_files :file_path

    File.open(output(:file_path), 'w'){|f| f.write PHASE_1_MSG}
  end

  def print
    required_input_files :file_path
    required_output_files

    msg = nil
    File.open(input(:file_path), 'r'){|f| msg = f.read}
    return msg
  end
end

class TestJob < Test::Unit::TestCase
  INPUT_PATH = 'tmp/input.txt'
  OUTPUT_PATH = 'tmp/output.txt'

  def job_config_basic
    {
      inputs: {
        read: INPUT_PATH,
      },
      outputs: {
        write: OUTPUT_PATH,
      },
      worker: {
        ruby_class: MyWorker,
        ruby_method: :generate,
      },
    }
  end

  def test_job_create
    job = Job.new(job_config_basic)
    assert_equal({read: INPUT_PATH}, job.inputs)
    assert_equal({write: OUTPUT_PATH}, job.outputs)
    assert_equal(MyWorker, job.ruby_class)
    assert_equal(:generate, job.ruby_method)
  end

  def test_job_lazy
    job = LazyJob.new(job_config_basic)
    File.delete(OUTPUT_PATH) if File.exist?(OUTPUT_PATH)
    sim = job.simulate
    assert_equal("MyWorker.new({:read=>\"tmp/input.txt\"}, {:write=>\"tmp/output.txt\"}).generate", sim, "When output files is missing, sim should generate it")

    File.open(OUTPUT_PATH, 'w'){|f| f.write('test')}
    sim = job.simulate
    assert_nil(sim, "When outout file exist, LazyJob.sim should do nothing")
  end
end

class TestPipeline < Test::Unit::TestCase

  def generate_job_config
    { # write a message to file_path
      inputs: {},
      outputs: {
        file_path: PHASE_1_PATH,
      },
      worker: {
        ruby_class: MyWorker,
        ruby_method: :generate,
      },
    }
  end

  def print_job_config
    { # print (and return) the contents of file_path
      inputs: {
        file_path: PHASE_1_PATH,
      },
      outputs: {
      },
      worker: {
        ruby_class: MyWorker,
        ruby_method: :print,
      },
    }
  end


  def pipeline_config_missing_job
    { # a print job that relies on an input file, will raise exception when input file does not exist
      jobs: {
        generate: generate_job_config,
      },
      job_order: [:generate, :print], # :print job is not defined above, so should cause an error
    }
  end

  def test_missing_job
    pipeline = Pipeline.new(pipeline_config_missing_job)

    assert_raise ArgumentError, "Should raise ArgumentError because the :print job is not defined in :jobs[]" do
      result = pipeline.run
    end
  end
     
  def pipeline_config_missing_input_file
    { # a print job that relies on an input file, will raise exception when input file does not exist
      jobs: {
        print: print_job_config,
      },
      job_order: [:print],
    }
  end

  def test_missing_input_file
    # remove input file, so we can test Exception handling
    File.delete(PHASE_1_PATH) if File.exist?(PHASE_1_PATH)

    pipeline = Pipeline.new(pipeline_config_missing_input_file)

    assert_raise ArgumentError, "Should raise ArgumentError because the input file does not exist" do
      result = pipeline.run
    end
  end
     
  def pipeline_config_basic
    {
      jobs: {
        generate: generate_job_config,
        print: print_job_config,
      },
      job_order: [:generate, :print],
    }
  end

  def test_basic_run
    # remove input file, in case an old copy was left behind
    File.delete(PHASE_1_PATH) if File.exist?(PHASE_1_PATH)

    pipeline = Pipeline.new(pipeline_config_basic)

    assert_nothing_raised "pipeline.run should work fine" do
      result = pipeline.run
      assert_equal(PHASE_1_MSG, result)
    end

    assert_file_contains(PHASE_1_MSG, PHASE_1_PATH)
    File.delete(PHASE_1_PATH)
  end
     
  def test_basic_simulate
    pipeline = Pipeline.new(pipeline_config_basic)
    result = pipeline.simulate
    expected_sim = [ # NOTE: adding escaped new-line chars to match the output from simulate()
      "MyWorker.new({}, {:file_path=>\"#{PHASE_1_PATH}\"}).generate",
      "MyWorker.new({:file_path=>\"#{PHASE_1_PATH}\"}, {}).print",
    ]
    assert_equal(expected_sim, result)
  end

end
