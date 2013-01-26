module Anubis
  class Operation
    #
    # Mixin how to 'delete'
    #
    def delete
      self.extend Delete
      perform
    end
    
    module Delete
      
      def validate
        true
      end

      def execute
        deletes = @row_keys.product(mapping)
        Connection.safely_send(:deleteAll, @table, row_key, columns, {})
      end

      def prepare_results
        true
      end
    end
  end
end
