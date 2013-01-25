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

    def to_s
      "#{key} x [ #{cells.map(&:fullname).join(', ')} ]"
    end
    
    #
    # Query operations
    #
    def get(versions = nil)
      Get.from_row(self, versions).perform
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
end
