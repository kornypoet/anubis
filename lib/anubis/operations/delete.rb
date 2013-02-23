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
        mutations = @row_keys.map do |key| 
          mutate = mapping.map{ |column| Apache::Hadoop::Hbase::Thrift::Mutation.new(column: column, isDelete: true) } 
          Apache::Hadoop::Hbase::Thrift::BatchMutation.new(row: key, mutations: mutate)
        end
        Anubis.connection.safely_send(:mutateRows, @table, mutations, {})        
      end

      def prepare_results
        true
      end
    end
  end
end
