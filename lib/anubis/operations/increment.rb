module Anubis
  class Operation
    #
    # Mixin how to 'increment'
    #
    def increment(amt = 1)
      self.extend Increment
      perform(amt)
    end
    
    module Increment
    
    attr_reader :table, :row_key, :column, :amt
    
    def self.from_row(row, amt)
      table   = row.table
      row_key = row.key
      cell    = row.cells.first
      columns = cell.fullname
      new(table, row_key, columns, amt)
    end
   
    def initialize(table, row_key, column, amt)
      @table   = table
      @row_key = row_key      
      @column  = column
      @amt     = amt
    end
    
    def perform
      results = Connection.atomicIncrement(table, row_key, column, amt)      
    end    
  end
end
