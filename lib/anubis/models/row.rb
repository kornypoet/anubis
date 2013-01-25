module Anubis
  class Row

    attr_reader :key, :cells, :table

    def self.from_result(table, thrift_result)
      key = thrift_result.row
      cells = thrift_result.columns.to_hash.inject([]) do |ary, (column, cell)|
        family, qualifier = column.split(':', 2)
        ary << Cell.new(family, qualifier, cell.value, cell.timestamp)
        ary
      end
      new(table, key, cells)
    end
    
    def initialize(table, key, cells)
      @table = table
      @key   = key
      @cells = cells
    end

    def to_hash
      { key: key, cells: cells.map(&:to_hash) }
    end
    
    def get
      Get.from_row(self).perform
    end

    def put value
      Put.from_row(self, value).perform
    end

    def increment(amt = 1)
      Increment.from_row(self, amt).perform
    end

    def delete
      Delete.from_row(self).perform
    end
    
  end

  class Cell
    
    attr_reader :column, :qualifier, :value, :timestamp
    
    def initialize(column, qualifier = nil, value = nil, timestamp = nil)
      @column    = column
      @qualifier = qualifier
      @value     = value
      @timestamp = timestamp
    end

    def fullname
      [column, qualifier.to_s].join(':')
    end

    def to_hash
      { 
        column:    column, 
        qualifier: qualifier,        
        value:     value,        
        timestamp: timestamp
      }
    end
  end
end
