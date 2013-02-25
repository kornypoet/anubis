require 'thrift'
require 'gorillib/some'

require 'anubis/thrift'
require 'anubis/client'

require 'anubis/models/table'
require 'anubis/models/table_schema'
require 'anubis/models/bloom_filter'
require 'anubis/models/column_details'
require 'anubis/models/scanner'

require 'anubis/operations'
require 'anubis/operations/get'
require 'anubis/operations/increment'
require 'anubis/operations/put'
require 'anubis/operations/delete'
require 'anubis/operations/scan'

require 'anubis/errors'

module Anubis
  
  class << self

    def connection 
      @connection ||= Client.new
    end
    
    def configure(&blk)
      if block_given?
        yield connection 
        connection.reset_thrift_protocol!
      end
      self
    end

    def connect!
      configure_from(deploy_config) if deploy_pack?
      configure_from(rails_config)  if rails?
      connection.connect
    end
    
    def tables
      connection.safely_send(:getTableNames).map{ |table| Table.from_existing table }
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

  private

    def operation params
      Operation.new(params[:table]).
        columns(*params[:columns]).
        qualifier(params[:qualifier]).
        rows(*params[:rows])        
    end

    def deploy_pack?
      defined?(Wukong::Deploy) && Wukong::Deploy.respond_to?(:booted?) && Wukong::Deploy.booted?
    end

    def rails?
      defined?(Rails) && Rails.respond_to?(:root) && Rails.root
    end

    def deploy_config
      host = Wukong::Deploy.settings[:hbase][:thrift][:host] rescue nil
      port = Wukong::Deploy.settings[:hbase][:thrift][:port] rescue nil
      { host: host, port: port }
    end

    def rails_config
      config = Rails.configuration.database_configuration
      host   = config[Rails.env]['thrift']['host'] rescue nil
      port   = config[Rails.env]['thrift']['host'] rescue nil
      { host: host, port: port }
    end

    def configure_from conf
      configure do |c|
        c.host = conf[:host] if conf[:host]
        c.port = conf[:port] if conf[:host]
      end
    end
  end
end
