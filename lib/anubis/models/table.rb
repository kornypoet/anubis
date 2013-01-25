module Anubis
  class Table

    attr_reader :name, :column_group, :column_details

    def self.find table
      from_existing(table) if Connection.safely_send(:getTableNames).include? table
    end      

    def self.find_or_create(table, *columns)
      if t = find(table)
        t
      else
        t = new(table, *columns)
        t.create
      end
      t
    end

    def self.from_existing table
      new(table, *ColumnDetails.from_existing(table))
    end
    
    def initialize(table_name, *details)
      @name           = table_name
      @column_details = details.map{ |col| col.is_a?(ColumnDetails) ? col : ColumnDetails.new(name: col.to_s) }
    end

    def create
      Connection.safely_send(:createTable, name, column_details)
      true
    end

    def update(options = {})
      raise NotImplementedError
    end
    
    def delete
      disable
      Connection.safely_send(:deleteTable, name)
      true
    end
    
    def exists?
      Connection.safely_send(:getTableNames).include? name
    end

    def enabled?
      exists? && Connection.safely_send(:isTableEnabled, name) 
    end

    def disable
      Connection.safely_send(:disableTable, name)
    end

    def enable
      Connection.safely_send(:enableTable, name)
    end
    
    def describe
      { name: name, columns: column_details.map(&:describe) }
    end

    def to_s
      "<#{self.class}[ #{name} ] => columns#{column_group.serialize}>"
    end

    #
    # Query operations
    #
    def column_group
      ColumnGroup.new(name, *column_details.map(&:pretty_name))
    end
    
    def columns(*names)
      column_group.select_by_name(*names)
    end
    
    def qualifier qual
      columns.qualifier qual
    end
    
    def row row_key
      columns.qualifier.row row_key
    end
  end
end
