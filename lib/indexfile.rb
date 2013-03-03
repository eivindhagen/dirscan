require File.expand_path('logging', File.dirname(__FILE__))
require 'json'
require 'bindata'

require File.join(File.dirname(__FILE__), 'symbolize_keys')

Encoding.default_internal = Encoding::UTF_8

class IndexFile
  # Mix in the ability to log stuff ...
  include Logging

  class Reader
    def initialize(file_path, &block)
      File.open(file_path, 'rb') do |f|
        @file = f
        block.call self
      end
    end

    def read_object()
      length = BinData::Int32be.new.read(@file)
      string = @file.read(length)
      # logger.debug string.encoding
      utf8_string = string.force_encoding(Encoding::UTF_8)
      # logger.debug string
      # logger.debug utf8_string.encoding
      # logger.debug utf8_string
      object = JSON.parse(utf8_string)
      # logger.debug JSON.pretty_generate(object)
      object.symbolize_keys
      object[:recursive].symbolize_keys if object[:recursive]
      return object
    end

    def eof?
      @file.eof?
    end
  end

  class Writer
    def initialize(file_path, &block)
      File.open(file_path, 'wb') do |f|
        @file = f
        block.call self
      end
    end

    def write_object(object)
      string = object.to_json
      num_bytes = string.bytesize
      BinData::Int32be.new(num_bytes).write(@file)
      @file.write string
    end

  end

end