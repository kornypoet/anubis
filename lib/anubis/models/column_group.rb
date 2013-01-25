module Anubis
  class ColumnGroup
    
    attr_reader :table_name, :group, :qual
    
    def initialize(table, *columns)
      @table_name = table
      @group      = columns     
    end

    def fullnames
      group.map{ |col| [col, qual].compact.join(':') }
    end

    def serialize(list = group)
      '[ ' + list.join(', ') + ' ]'
    end
    
    def to_s
      serialize fullnames
    end

    #
    # Query operations
    #
    def select_by_name(*names)
      return self if names.empty?
      columns = names.map{ |name| group.detect{ |col| col == name.to_s } }.compact
      self.class.new(table_name, *columns)
    end

    def qualifier(name = nil)
      @qual = name.to_s
      self
    end

    def to_cells
      group.map{ |col| Cell.new(col, qual) }
    end

    def row key
      Row.new(table_name, key, self.to_cells)
    end
    
  end
end
