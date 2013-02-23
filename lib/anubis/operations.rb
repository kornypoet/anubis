require 'anubis/operations/get'
require 'anubis/operations/increment'
require 'anubis/operations/put'
require 'anubis/operations/delete'
require 'anubis/operations/scan'

module Anubis
  class Operation
    
    def initialize(table, columns = [], row_keys = [])
      @table    = table
      @columns  = columns
      @row_keys = row_keys
    end

    def columns(*cols)
      @columns = cols
      self
    end

    def qualifier(qual)
      @qualifier = qual
      self
    end

    def rows(*keys)
      @row_keys = keys
      self
    end
      
    def mapping
      @qualifier ? @columns.map{ |col| [col, @qualifier].join(':') } : @columns
    end

    def to_s
      "Op: #{@table} | [ #{@row_keys.join(', ')} ] x [ #{mapping.join(', ')} ]"
    end

    def validate
      t = Table.find(@table) or raise Anubis::NonexistentTableError, @table
      @columns.each do |col| 
        t.column_group.include? col or raise Anubis::NonexistentColumnError, col
      end
      true
    end
    
    def perform
      validate
      execute
      prepare_results
    end
    
  end
end
