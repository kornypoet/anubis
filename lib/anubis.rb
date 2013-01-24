require 'thrift'

require 'anubis/thrift'
require 'anubis/client'
require 'anubis/models'

module Anubis

  Connection = Client.new unless defined? Connection  
  
  class << self

    def configure(&blk)
      yield Connection if block_given?
      Connection.reset_thrift_protocol!
      self
    end
    
    def list_tables
      Connection.getTableNames
    end
    
    def connect!
      Connection.connect!
    end

  end
end
