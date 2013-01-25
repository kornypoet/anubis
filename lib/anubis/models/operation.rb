module Anubis  
  class Get

    attr_reader :table, :row_key, :columns

    def self.from_row(row, versions)
      table   = row.table
      row_key = row.key
      columns = row.cells.empty? ? nil : row.cells.map(&:fullname)
      new(table, row_key, columns, versions)
    end
   
    def initialize(table, row_key, columns, versions)
      @table    = table
      @row_key  = row_key      
      @columns  = columns
      @versions = versions
    end
    
    def perform
      if versions
        results = Connection.getRowWithColumns(table, row_key, columns, {})
      else
        results = Connection.getRowWithColumns(table, row_key, columns, {})
      end
      Row.from_result(table, results.first)
    end

  end
  
  class Put

    attr_reader :table, :row_key, :mutation

    def self.from_row(row, value)
      cell    = row.cells.first
      table   = row.table
      row_key = row.key
      columns = cell.fullname
      new(table, row_key, columns, value)
    end

    def initialize(table, row_key, column, value)
      @table    = table
      @row_key  = row_key     
      @mutation = [Apache::Hadoop::Hbase::Thrift::Mutation.new(column: column, value: value)]
    end
    
    def perform
      Connection.mutateRow(table, row_key, mutation, {})
      true
    end

  end
  
  class Increment
    
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
  
  class Delete
    
    attr_reader :table, :row_key, :columns
    
    def self.from_row row
      table   = row.table
      row_key = row.key
      columns = row.cells.first.fullname rescue nil
      new(table, row_key, columns)
    end
   
    def initialize(table, row_key, columns)
      @table   = table
      @row_key = row_key      
      @columns = columns
    end
    
    def perform
      if columns
        Connection.deleteAll(table, row_key, columns, {})
      else
        Connection.deleteAllRow(table, row_key, {})
      end
      true
    end
  end
end
