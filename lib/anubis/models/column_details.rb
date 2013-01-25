module Anubis
  class ColumnDetails < Apache::Hadoop::Hbase::Thrift::ColumnDescriptor
    
    def self.from_existing table
      Connection.safely_send(:getColumnDescriptors, table).map do |key, column|
        new(
            name:         column.name,
            versions:     column.maxVersions, 
            compression:  column.compression,
            in_memory:    column.inMemory,
            ttl:          column.timeToLive,
            cached:       column.blockCacheEnabled,
            bloom_filter: BloomFilter.new(column.bloomFilterType, column.bloomFilterVectorSize, column.bloomFilterNbHashes)
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
      @bloom_filter              = options[:bloom_filter] || BloomFilter::Default.new
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

  end  
end
