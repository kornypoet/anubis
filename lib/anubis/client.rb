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

    def safely_send(message, *args)
      begin
        response = send(message, *args)
        response = true if response.nil?
        response
      rescue => e
        warn e.message
        reconnect
        false
      end
    end

    def to_s
      "#<#{self.class}:#{object_id} host:#{host.inspect} port:#{port}>"
    end
  end
end
