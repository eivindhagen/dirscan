# unit_tests.rb
#
# http://en.wikibooks.org/wiki/Ruby_Programming/Unit_testing
#
require "test/unit"
require 'redgreen'

#include the classes we are testing
require File.expand_path('../lib/pipeline.rb', File.dirname(__FILE__))

def assert_file_contains(expected, file_path, message = nil)
  assert(File.exist?(file_path), "File #{file_path} should exist")

  expected_size = expected.size
  actual_size = File.size(file_path)
  assert_equal(expected_size, actual_size, "File #{file_path} should be #{expected_size} bytes in size.")
  File.open(file_path) do |f|
    actual = f.read
    assert_equal(expected, actual, "File #{file_path} should contain the expected data.")
  end
end

PHASE_1_MSG = "This is phase 1 of the test."
PHASE_1_PATH = "tmp/pipeline_test_phase1.txt"

class TestWorker < NaiveWorker
  def generate
    File.open(@outputs[:file_path], 'w'){|f| f.write PHASE_1_MSG}
  end

  def print
    msg = nil
    File.open(@inputs[:file_path], 'r'){|f| msg = f.read}
    puts msg
    return msg
  end
end

class TestPipeline < Test::Unit::TestCase

  def pipeline_config_basic
    {
      jobs: {
        generate: { # write a message to file_path
          inputs: {},
          outputs: {
            file_path: PHASE_1_PATH,
          },
          worker: {
            ruby_class: TestWorker,
            ruby_method: :generate,
          },
        },
        print: { # print (and return) the contents of file_path
          inputs: {
            file_path: PHASE_1_PATH,
          },
          outputs: {
          },
          worker: {
            ruby_class: TestWorker,
            ruby_method: :print,
          },
        },
      },
      job_order: [:generate, :print],
    }
  end

  def test_basic_run
    pipeline = Pipeline.new(pipeline_config_basic)
    result = pipeline.run
    assert_equal(PHASE_1_MSG, result)
    assert_file_contains(PHASE_1_MSG, PHASE_1_PATH)
    File.delete(PHASE_1_PATH)
  end
     
  def test_basic_simulate
    pipeline = Pipeline.new(pipeline_config_basic)
    result = pipeline.simulate
    expected_sim = [ # NOTE: adding escaped new-line chars to match the output from simulate()
      "TestWorker.new({}, {:file_path=>\"#{PHASE_1_PATH}\"}).generate",
      "TestWorker.new({:file_path=>\"#{PHASE_1_PATH}\"}, {}).print",
    ]
    assert_equal(expected_sim, result)
  end
     
end
