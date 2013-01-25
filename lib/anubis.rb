require 'thrift'
require 'gorillib/some'

require 'anubis/thrift'
require 'anubis/client'
require 'anubis/models'
require 'anubis/operations'

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

    def get(options = {})
      Operation.new(options[:table]).
        columns(options[:columns]).
        qualifier(options[:qualifier]).
        rows(options[:rows]).
        get
    end
    
  end
end
