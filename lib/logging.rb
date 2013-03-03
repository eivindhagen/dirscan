require 'logger'

# module Logging
#   # This is the magical bit that gets mixed into your classes
#   def logger
#     Logging.logger
#   end

#   # Global, memoized, lazy initialized instance of a logger
#   def self.logger
#     @logger ||= Logger.new(STDOUT)
#   end
# end

module Logging
  # extend base class by adding these class methods
  module ClassMethods
    def logger
      @logger ||= Logging.logger_for(self.class.name)
    end
  end

  def self.included(base)
    base.extend(ClassMethods)
  end


  # extend base class by adding these instance methods
  def logger
    @logger ||= Logging.logger_for(self.class.name)
  end


  # Use a hash class-ivar to cache a unique Logger per class:
  @loggers = {}
  @default_log_level = Logger::INFO

  class << self
    def set_default_log_level(level)
      @default_log_level = level
    end

    def logger_for(classname)
      @loggers[classname] ||= configure_logger_for(classname)
    end

    def configure_logger_for(classname)
      logger = Logger.new(STDOUT)
      logger.level = @default_log_level
      logger.progname = classname
      logger
    end
  end
end