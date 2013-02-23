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
        super
        true        
      end

      def versioned_get
        versioned_gets = @row_keys.product(mapping)        
        @results = versioned_gets.inject({}) do |result, (row_key, column)|
          cells = Anubis.connection.safely_send(:getVer, @table, row_key, column, @get_versions, {})
          unless cells.empty?
            cells.map!{ |cell| { column: column, value: cell.value, timestamp: cell.timestamp } }
          end
          result[row_key] = cells
          result
        end
      end
      
      def columned_get
        columns  = mapping.empty? ? nil : mapping
        @results = Anubis.connection.safely_send(:getRowsWithColumns, @table, @row_keys, columns, {})
        if @results.empty?        
          puts "oh shit"
        else
          @results = @results.inject({}) do |prep, data| 
            if data.is_a?(Apache::Hadoop::Hbase::Thrift::TRowResult)
              prep[data.row] = data.columns.map do |column, cell| 
                { 
                  column:    column, 
                  value:     cell.value, 
                  timestamp: cell.timestamp 
                }
              end
              prep
            end            
          end
        end
      end

      def execute
        (@get_versions && @get_versions.is_a?(Numeric)) ? versioned_get : columned_get
      end
      
      def prepare_results
        @results
      end    
    end
  end
end
