module Anubis
  class Operation
    #
    # Mixin how to 'increment'
    #
    def increment(amt = 1)
      self.extend Increment
      set_amount amt
      perform
    end
    
    module Increment

      def set_amount amt
        @put_amount = amt
      end

      def validate
        true
      end
      
      def execute
        increments   = @row_keys.product(mapping)        
        @next_values = increments.inject({}) do |result, (key, column)|
          # This has to be done iteratively because the batch increment is broken in Thrift
          result[key] = Connection.safely_send(:atomicIncrement, @table, key, column, @put_amount)      
          result
        end
      end
      
      def prepare_results
        @next_values
      end
    end
  end
end
