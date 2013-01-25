module Anubis
  class Table

    def self.find name
      from_existing(name) if Connection.getTableNames.include? name
    end      

    def self.find_or_create(name, *columns)
      if table = find(name)
        table
      else
        table = new(name, *columns)
        table.create
      end
      table
    end

    def self.from_existing table
      t = new(table)
      t.columns = ColumnGroup.from_existing(table)
      t
    end
    
    def initialize(name, *columns)
      @name    = name
      @columns = ColumnGroup.new(name, *columns)
    end

    def columns= group
      @columns = group
    end

    def columns(*names)
      @columns.select_by_name(*names)
    end

    def create
      Connection.createTable(@name, columns.to_a)
      true
    end

    def update(options = {})
      raise NotImplementedError
    end
    
    def row(row_key)
      columns.qualifier.row(row_key)
    end
    
    def qualifier(qual)
      columns.qualifier(qual)
    end
    
    def delete
      disable
      Connection.deleteTable @name
      true
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
      { name: @name, columns: @columns.describe }
    end

    def to_s
      "#<#{self.class} name:#{@name}, columns:#{@columns}>"
    end
    
  end
end
