module Anubis
  class Operation
    #
    # Mixin how to 'delete'
    #
    def delete
      self.extend Delete
      self.perform
    end
    
    module Delete
    
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
end
