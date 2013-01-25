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

    def connect!
      Connection.safely_send(:connect)
    end
    
    def tables
      Connection.safely_send(:getTableNames).map{ |table| Table.from_existing table }
    end
    
    def operation params
      Operation.new(options[:table]).
        columns(options[:columns]).
        qualifier(options[:qualifier]).
        rows(options[:rows])        
    end
    
    def get(params = {})
      operation(params).get
    end

    def put(params = {})
      operation(params).put
    end

    def increment(params = {})
      operation(params).increment
    end
    
    def delete(params = {})
      operation(params).delete
    end

    def scan(params = {})
      operation(params).scan
    end
  end
end
