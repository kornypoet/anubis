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

    def deploy_pack?
      defined?(Wukong::Deploy) && Wukong::Deploy.respond_to?(:booted?) && Wukong::Deploy.booted?
    end

    def configure_from_deploy
      host = Wukong::Deploy.settings[:hbase][:thrift][:host] rescue nil
      port = Wukong::Deploy.settings[:hbase][:thrift][:port] rescue nil
      configure do |c|
        c.host = host if host
        c.port = port if port
      end
    end

    def connect!
      configure_from_deploy if deploy_pack?
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
      operation(params).get params[:versions]
    end

    def put(params = {})
      operation(params).put params[:value]
    end

    def increment(params = {})
      operation(params).increment params[:amount]
    end
    
    def delete(params = {})
      operation(params).delete
    end

    def scan(params = {})
      operation(params).scan params
    end
  end
end
