module Anubis
  class Column < Apache::Hadoop::Hbase::Thrift::ColumnDescriptor

    attr_accessor :qualifier, :table_name
    
    def self.from_existing(table, column)
      new(
          table_name:   table,
          name:         column.name,
          versions:     column.maxVersions, 
          compression:  column.compression,
          in_memory:    column.inMemory,
          ttl:          column.timeToLive,
          cached:       column.blockCacheEnabled,
          bloom_filter: DefaultBloomFilter.new(column.bloomFilterType, column.bloomFilterVectorSize, column.bloomFilterNbHashes)
          )     
    end

    def initialize(options = {})
      @table_name                = options[:table_name]
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

    def row(key = nil)
      Row.new(table_name, key, [self.to_cell])
    end

    def to_cell
      Cell.new(pretty_name, qualifier || '')
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

  class ColumnGroup
    
    attr_reader :table_name
    
    def self.from_existing table
      existing = Connection.getColumnDescriptors table
      columns  = existing.map{ |key, column| Column.from_existing(table, column) }
      new(table, *columns)
    end

    def initialize(table, *columns)
      @table_name = table
      @group = columns.map{ |c| c.is_a?(Column) ? c : Column.new(name: c.to_s, table_name: table) }
    end

    def select_by_name(*names)
      return self if names.empty?
      columns = names.map{ |name| @group.detect{ |col| col.pretty_name == name.to_s } }.compact
      self.class.new(table_name, *columns)
    end

    def to_a
      @group
    end

    def to_s
      @group.to_s
    end

    def qualifier(qual = nil)
      @group.each{ |col| col.qualifier = qual.to_s }
      self
    end

    def row key
      Row.new(table_name, key, @group.map(&:to_cell))
    end
    
  end
end
