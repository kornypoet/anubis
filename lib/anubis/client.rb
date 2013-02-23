module Anubis  
  class Client < Apache::Hadoop::Hbase::Thrift::Hbase::Client
    attr_accessor :host, :port 
    
    def initialize(options = {})
      @host  = options[:host] || 'localhost'
      @port  = options[:port] || 9090
      @seqid = 0
      reset_thrift_protocol!
    end

    def reset_thrift_protocol!
      @socket         = Thrift::Socket.new(host, port)
      @transport      = Thrift::BufferedTransport.new(@socket)
      @iprot = @oprot = Thrift::BinaryProtocol.new(@transport)
      self
    end
    
    def connect
      @transport.open
      true
    rescue => e
      @transport.close
      raise Anubis::ConnectionError, e.message
    end    
    
    def connected?
      @transport.open?
    end

    def disconnect
      @transport.close
      true
    end

    def reconnect
      disconnect if connected?
      connect
    end

    def to_s
      "#<#{self.class}:#{object_id} host:#{host.inspect} port:#{port}>"
    end
    
    def safely_send(message, *args)
      raise Anubis::ConnectionError, 'Cannot perform that operation unless connected to HBase' unless connected?
      begin
        handle_response(message, *args)
      rescue => e
        handle_error(e, message, *args)
      end
    end

    def handle_response(message, *args)
      response = send(message, *args)
      response = true if response.nil?
      response      
    end

    def handle_error(err, message, *args)
      warn err.message
      reconnect
      false      
    end
  end
end
