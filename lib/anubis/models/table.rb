module Anubis
  class Table

    attr_reader :name, :column_details

    #
    # Class-level lookup
    #
    class << self
      def list
        return @list if @list
        reload_tables
      end
      
      def reload_tables
        @list = Anubis.connection.safely_send(:getTableNames).map{ |table| from_existing table }
      end

      def find table
        list.detect{ |t| t.name == table }
      end      
      
      def find_or_create(table, *columns)
        if t = find(table)
          t
        else
          t = new(table, *columns)
          t.create
        end
        t
      end
      
      def from_existing table
        new(table, *ColumnDetails.from_existing(table))
      end
    end

    def initialize(table_name, *details)
      @name           = table_name
      @column_details = details.map{ |col| col.is_a?(ColumnDetails) ? col : ColumnDetails.new(name: col.to_s) }
    end

    #
    # CRUD operations
    #
    def create
      Anubis.connection.safely_send(:createTable, name, column_details) and refresh
    end

    def update(options = {})
      raise NotImplementedError
    end
    
    def delete
      disable and Anubis.connection.safely_send(:deleteTable, name) and refresh
    end
    
    def exists?
      self.class.list.include? self
    end

    def enabled?
      exists? && Anubis.connection.safely_send(:isTableEnabled, name) 
    end

    def disable
      Anubis.connection.safely_send(:disableTable, name)
    end

    def enable
      Anubis.connection.safely_send(:enableTable, name)
    end

    def refresh
      !!self.class.reload_tables
    end

    #
    # Information
    #
    def describe
      { name: name, columns: column_details.map(&:describe) }
    end

    def column_group
      column_details.map(&:pretty_name)
    end

    def to_s
      "<#{self.class}[#{name}] => columns[#{column_group.join(', ')}]>"
    end

    #
    # Operation builder
    #
    def operation
      # Use self here
      Operation.new(name)
    end

    def select_by_name names
      return column_group if names.empty?
      names.map{ |name| column_group.detect{ |col| col == name.to_s } }.compact      
    end

    def columns(*names)      
      operation.columns(*select_by_name(names))
    end
    
    def qualifier(qual = nil)
      columns.qualifier(qual)
    end

    def rows(*keys)
      qualifier.rows(*keys)
    end
    
    def scan(options = {}, &blk)
      qualifier.scan(options, &blk)
    end
  end
end
