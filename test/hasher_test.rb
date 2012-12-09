# unit_tests.rb
#
# http://en.wikibooks.org/wiki/Ruby_Programming/Unit_testing
#
require "test/unit"
require 'redgreen'

#include the classes we are testing
require File.expand_path('../lib/hasher.rb', File.dirname(__FILE__))

class TestHasher < Test::Unit::TestCase

	def test_hasher_blank
		h = Hasher.new('', {})
    assert_equal('', h.source )

    verified_result = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'	# echo -ne '' | shasum -a 256
    assert_equal(verified_result, h.hash )
	end
	
	def test_hasher_nohash
		h = Hasher.new('k', {:l => 'lost'})	# info hash does not have the key 'k'
    assert_equal('', h.source )

    verified_result = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'	# echo -ne '' | shasum -a 256
    assert_equal(verified_result, h.hash )
	end
	
	def test_hasher_x
		info = {
			:x => 'cross',
		}
		template = 'x'
		h = Hasher.new(template, info)
    assert_equal('cross', h.source )

    verified_result = 'b3986952b145da5f0e4bd416f3b948e9864b57895675886c1deb83c16a9beadc'	# echo -ne 'cross' | shasum -a 256
    assert_equal(verified_result, h.hash )
	end
 
	def test_hasher_abc

		info = {
			:a => 'A',
			:b => 'BB',
			:c => 'CCC',
		}
		template = 'a+b+c'
		h = Hasher.new(template, info)
    assert_equal('A+BB+CCC', h.source )

    verified_result = '5c9074c8caaddb711e59aad51cbf427c79f9676bc2b7d89b4ef75a32279dbaf3'	# echo -ne 'A+BB+CCC' | shasum -a 256
    assert_equal(verified_result, h.hash )
	end
 
end
