module Anubis
  class Operation
    #
    # Mixin how to 'scan'
    #
    def scan(options = {}, &processor)
      self.extend Scan
      set_options   options
      set_processor processor if block_given?
      perform
    end
    
    module Scan

      def set_options opts
        @scan_options = opts
      end

      def set_processor processor
        @scan_processor = processor
      end
      
      def batch_size
        @scan_options[:batch] || 1
      end
      
      def validate
        true
      end

      def create_scanner
        Connection.safely_send(:scannerOpenWithStop, @table, @scan_options[:from], @scan_options[:to], mapping, {})
      end

      def execute
        scanner = create_scanner
        until (results = Connection.safely_send(:scannerGetList, scanner, batch_size)).empty?
          @scan_processor.call(results) if @scan_processor
        end
      end
      
      def prepare_results
        true
      end
    end
  end
end
