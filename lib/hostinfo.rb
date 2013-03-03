require File.expand_path('logging', File.dirname(__FILE__))
require 'socket'

class HostInfo
  # Mix in the ability to log stuff ...
  include Logging

  def self.name
      Socket.gethostname
  end

end