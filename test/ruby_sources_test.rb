# unit_tests.rb
#
# http://en.wikibooks.org/wiki/Ruby_Programming/Unit_testing
#
require "test/unit"
require 'redgreen'

#include the classes we are testing
require File.expand_path('../lib/dirscan_worker.rb', File.dirname(__FILE__))

class TestRubySources < Test::Unit::TestCase

  def test_use_spaces_not_tabs
    Dir.glob("**/*.rb").each do |path|
      text = File.read(path)
      match = /\t/.match(text)
      assert_nil(match, "Found tab-char in '#{path}'")
    end
  end
     
end
