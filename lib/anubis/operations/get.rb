module Anubis
  class Operation    
    #
    # Mixin how to 'get'
    #
    def get(versions = nil)
      self.extend Get
      set_versions versions
      perform
    end

    module Get

      def set_versions amt
        @get_versions = amt
      end

      def validate
        true
      end

      def execute
        if @get_versions && @get_versions.is_a?(Numeric)
          versioned_gets = @row_keys.product(mapping)
          @results = versioned_gets.map do |row_key, column|
            Connection.safely_send(:getVer, @table, row_key, column, @get_versions, {})
          end
        else
          columns  = mapping.empty? ? nil : mapping
          @results = Connection.safely_send(:getRowsWithColumns, @table, @row_keys, columns, {})
        end
      end
      
      def prepare_results
        @results
      end

    end    
  end
end
