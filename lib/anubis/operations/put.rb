module Anubis
  class Operation
    #
    # Mixin how to 'put'
    #
    def put val
      self.extend Put
      set_value val
      self.perform
    end
    
    module Put

      def set_value val
        @put_value = val
      end
      
      def validate
        true
      end
      
      def execute
        mutations = @row_keys.map do |key| 
          mutate = mapping.map{ |column| Apache::Hadoop::Hbase::Thrift::Mutation.new(column: column, value: @put_value) } 
          Apache::Hadoop::Hbase::Thrift::BatchMutation.new(row: key, mutations: mutate)
        end
        Connection.safely_send(:mutateRows, @table, mutations, {})        
      end
      
      def prepare_results
        true
      end
    end
  end
end
