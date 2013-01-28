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

      def execute
        scanner  = Anubis::Scanner.new(@table, @scan_options[:from], @scan_options[:to], mapping, batch_size)
        # If given a processor, iterate over the scan using that, otherwise return the scanner
        @results = @scan_processor ? scanner.each_batch(&@scan_processor) : scanner          
      end
      
      def prepare_results
        @results
      end
    end
  end
end
