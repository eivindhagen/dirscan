# IMPORTANT: Must require 'simplecov' BEFORE require 'test/unit' or else it won't be able to record coverage for the actual test run,
#            instead we only get the coverage from the parsing of the source files (not the code execution)
require 'simplecov'

# start the coverage system
SimpleCov.start do
  add_filter '/test/' # remove all files in the test/ folder form the generated report
  # command_name "random_#{rand(100)}"
end

require 'test/unit'
require 'redgreen'
