require 'socket'

class HostInfo

  def self.name
      Socket.gethostname
  end

end