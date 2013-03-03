require File.expand_path('logging', File.dirname(__FILE__))
require 'digest'

HASH_SRC_SPLIT = '+'
HASH_SRC_JOIN = '+'

class StringHash
  # Mix in the ability to log stuff ...
  include Logging

  def self.md5(string)
    Digest::MD5.hexdigest(string.to_s)
  end

  def self.sha256(string)
    Digest::SHA256.hexdigest(string.to_s)
  end
end

class FileHash
  # Mix in the ability to log stuff ...
  include Logging

  def self.md5(file_path)
    begin
      Digest::MD5.file(file_path).hexdigest
    rescue
      nil
    end
  end

  def self.sha256(file_path)
    begin
      Digest::SHA256.file(file_path).hexdigest
    rescue
      nil
    end
  end
end

class Hasher
  # Mix in the ability to log stuff ...
  include Logging

  # replaces each <entry> found in the hash_template with it's corresponding value from the info hash
  # NOTE: the info hash keys should be symbols (NOT string).
  def initialize(template, info)
    keys = template.split(HASH_SRC_SPLIT).map{|key_string| key_string.to_sym}
    values = keys.map{|k| info[k]}
    @source = values.join(HASH_SRC_JOIN)
    @hash = StringHash.sha256(source)
    return self
  end

  def source
    return @source
  end

  def hash
    return @hash
  end
end
