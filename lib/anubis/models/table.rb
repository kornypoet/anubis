module Anubis
  class Table

    def self.find name
      if Connection.getTableNames.include? name       
        new(name, *Column.from_existing_columns(name)) 
      end
    end      

    def self.find_or_create(name, *columns)
      table = find(name) || new(name, *columns)
      table.create
    end
    
    def initialize(name, *columns)
      @name    = name
      @columns = columns.map{ |c| c.is_a?(Column) ? c : Column.new(name: c.to_s) }
    end

    def create
      Connection.createTable(@name, @columns)
    end

    def delete
      disable
      Connection.deleteTable @name
    end
    
    def exists?
      Connection.getTableNames.include? @name
    end

    def enabled?
      exists? && Connection.isTableEnabled(@name)
    end

    def disable
      Connection.disableTable @name
    end

    def enable
      Connection.enableTable @name
    end
    
    def describe
      { name: @name, columns: @columns.map(&:describe) }
    end

    def to_s
      "#<#{self.class}:#{object_id} name:#{@name}, columns:#{@columns}>"
    end
    
  end
end
