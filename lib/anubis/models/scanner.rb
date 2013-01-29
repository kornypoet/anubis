module Anubis
  class Scanner

    attr_reader :batch_size, :scan_id

    def initialize(table, start_row, end_row, columns, batch_size)
      @batch_size = batch_size
      @scan_id    = Connection.safely_send(:scannerOpenWithStop, table, start_row, end_row, columns, {})
      @open       = true
    end

    def has_next?
      @open
    end
    
    def next_batch
      return nil unless has_next?
      batch = Connection.safely_send(:scannerGetList, scan_id, batch_size)
      batch.empty? ? close : parse_results(batch)
    end

    def each_batch(&blk)
      while batch = next_batch
        blk.call(batch) if block_given?
      end
    end

    def parse_results batch
      batch.inject({}) do |prep, data| 
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

    def close
      @open = false
      Connection.safely_send(:scannerClose, scan_id)
      nil
    end
    
  end
end
