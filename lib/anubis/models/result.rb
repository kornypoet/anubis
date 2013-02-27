module Anubis
  class Result

    attr_reader :raw
    
    def initialize(options = {})
      @raw = options[:raw]      
    end

    def [] column
      raw[column]
    end
    
  end
end
