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
    
    def tables
      Connection.safely_send(:getTableNames).map{ |table| Table.from_existing table }
    end
    
    def connect!
      Connection.safely_send(:connect)
    end

  end
end
