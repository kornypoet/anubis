module Anubis
  class Column < Apache::Hadoop::Hbase::Thrift::ColumnDescriptor
    
    def self.from_existing_columns table
      columns = Connection.getColumnDescriptors table
      columns.map do |key, col| 
        new(
            name:         col.name,
            versions:     col.maxVersions, 
            compression:  col.compression,
            in_memory:    col.inMemory,
            ttl:          col.timeToLive,
            cached:       col.blockCacheEnabled,
            bloom_filter: DefaultBloomFilter.new(col.bloomFilterType, col.bloomFilterVectorSize, col.bloomFilterNbHashes)
            )
      end
    end

    def initialize(options = {})
      self.name                  = options[:name]
      self.maxVersions           = options[:versions]     || 3
      self.compression           = options[:compression]  || 'NONE'
      self.inMemory              = options[:in_memory]    || false
      self.timeToLive            = options[:ttl]          || -1
      self.blockCacheEnabled     = options[:cached]       || false
      @bloom_filter              = options[:bloom_filter] || DefaultBloomFilter.new
      self.bloomFilterType       = @bloom_filter.type
      self.bloomFilterVectorSize = @bloom_filter.vector_size
      self.bloomFilterNbHashes   = @bloom_filter.hashes
    end

    def pretty_name
      name.gsub(/:/, '')
    end

    def describe
      {
        name:         pretty_name,
        versions:     maxVersions, 
        compression:  compression,
        in_memory:    inMemory,
        ttl:          timeToLive,
        cached:       blockCacheEnabled,
        bloom_filter: @bloom_filter.describe 
      }      
    end

    def to_s
      "<Column: #{pretty_name}>"
    end

    def inspect
      "<Column: #{pretty_name}>"
    end
  end
  
  class DefaultBloomFilter
    attr_accessor :type, :vector_size, :hashes

    def intialize(type, vector_size, hashes)
      @type        = type        || 'NONE'
      @vector_size = vector_size || 0
      @hashes      = hashes      || 0
    end

    def describe
      {
        type:        type,
        vector_size: vector_size,
        hashes:      hashes
      }
    end
  end
end
