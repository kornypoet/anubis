module Anubis
  class TableSchema

    DefinedSchema = [] unless defined? DefinedSchema
    
    class << self
      def inherited klass
        DefinedSchema << klass
      end
      
      def column_details
        @column_details ||= []
      end
      
      def column(name, &column_def)
        details = ColumnDetails.new(name: name.to_s)
        details.instance_eval(&column_def) if column_def
        column_details << details
      end
      
      def table_name
        self.to_s.underscore
      end
      
      def to_table
        Anubis::Table.new(table_name, *column_details)
      end
    end

  end
end
