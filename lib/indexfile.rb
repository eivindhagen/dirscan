require 'json'
require 'bindata'

require File.join(File.dirname(__FILE__), 'symbolize_keys')

class IndexFile

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
      object = JSON.parse(string)
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
      length = string.size
      BinData::Int32be.new(length).write(@file)
      @file.write string
    end

  end

end