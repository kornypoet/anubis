module Anubis
  class BloomFilter

    attr_accessor :type, :vector_size, :hashes

    def initialize(type, vector_size, hashes)
      @type        = type
      @vector_size = vector_size
      @hashes      = hashes
    end

    def describe
      {
        type:        type,
        vector_size: vector_size,
        hashes:      hashes
      }
    end
    
    class Default < BloomFilter
   
      def initialize(*args)
        super('NONE', 0, 0)
      end
      
    end    
  end
end
