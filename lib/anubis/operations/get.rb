module Anubis
  class Operation    
    #
    # Mixin how to 'get'
    #
    def get
      self.extend Get
      perform
    end

    module Get

      def validate
        true
      end

      def execute
        columns  = mapping.empty? ? nil : mapping
        @results = Connection.safely_send(:getRowsWithColumns, @table, @row_keys, columns, {})
      end
      
      def prepare_results
        @results
      end

    end    
  end
end
