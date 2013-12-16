module Anubis
  class Result

    attr_reader :raw, :operation, :operation_method
    
    def initialize(op, op_method, db_result)
      @operation        = op
      @operation_method = op_method
      @raw              = db_result
    end

    def [] column
      versions[column].current
    end

    def row_key
      @row_key  ||= raw.keys.first
    end
      
    def columns
      @columns  ||= raw[row_key].keys
    end

    def versions
      @versions ||= raw[row_key].inject({}){ |hsh, (col, cells)| hsh[col] = CellCollection.new(cells) ; hsh }
    end

    def each_version(column, &blk)
      versions[column].each(&blk)
    end
  end

  class CellCollection
    
    def initialize cells
      @cells = cells.map{ |data| Cell.new(data) }
      reset_cursor
    end

    def position
      @cursor
    end

    def rewind
      reset_cursor
    end
    
    def current
      @cells[@cursor]
    end

    def next
      cell = current
      increment_cursor
      cell
    end

    def each(&iterator)
      if block_given?
        reset_cursor
        @cells.each do |cell|
          iterator.call(cell, @cursor)
          increment_cursor
        end
        reset_cursor
      end
      self      
    end

  private

    def reset_cursor
      @cursor = 0
    end

    def increment_cursor
      @cursor += 1
    end
  end

  class Cell

    attr_reader :value, :timestamp
    
    def initialize(data = {})
      @value     = data[:value]
      @timestamp = data[:timestamp]
    end

    def raw
      { value: @value, timestamp: @timestamp }
    end
  end
end

