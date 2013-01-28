module Anubis
  class Operation
    #
    # Mixin how to 'scan'
    #
    def scan(options = {})
      self.extend Scan
      set_options options
      perform
    end
    
    module Put

      def set_options opts
        @scan_options = opts
      end
      
      def validate
        true
      end
      
      def execute
      end
      
      def prepare_results
        true
      end
    end
  end
end
