module Anubis
  class Operation
    module Put

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
end
