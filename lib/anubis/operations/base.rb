module Anubis
  class Operation

    attr_accessor :table
    
    def initialize(options = {})
      @table      = options[:table]
      @columns    = options[:columns]    || []
      @qualifiers = options[:qualifiers] || []
      @rows       = options[:rows]
    end
      
    def columns(*cols)
      return @columns if cols.empty?
      @columns = cols
      self
    end

    def qualifiers(*quals)
      return @qualifiers if quals.nil?
      @qualifers = quals
      self
    end
        
    def row(key = nil)
      return @row if key.nil?
      @row = key
      self
    end

    def mapping
      qualifiers.empty? ? columns : columns.product(qualifiers).join(':')
    end

    def validate
      t = Table.find(table) or raise Anubis::NonexistentTableError, table
      columns.each do |col| 
        t.column_group.include? col.to_s or raise Anubis::NonexistentColumnError, col
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
