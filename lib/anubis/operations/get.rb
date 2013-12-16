module Anubis
  class Operation    
    #
    # Mixin how to 'get'
    #
    def get(*parameters)
      options   = parameters.extract_options!
      @versions = options.delete(:versions)
      if parameters.empty?
        qualifier = options.keys.first
        rows(options[qualifier])
      else
        rows(parameters)
      end
      perform
    end

    module Get

      def validate
        super and !rows.blank?        
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
        cols = mapping.empty? ? nil : mapping
        @results = Anubis.connection.safely_send(:getRowsWithColumns, table, rows, cols, {})
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
        (@versions && @versions.is_a?(Numeric)) ? versioned_get : columned_get
      end
      
      def prepare_results
        @results
      end    
    end
    include Get
  end
end
