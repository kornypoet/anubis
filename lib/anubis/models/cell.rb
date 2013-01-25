module Anubis
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
