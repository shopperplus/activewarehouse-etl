module ETL #:nodoc:
  module Processor #:nodoc:
    class CheckScdProcessor < ETL::Processor::RowProcessor
      attr_accessor :primary_key

      attr_accessor :target

      attr_accessor :table

      attr_accessor :columns

      attr_accessor :latest_version
      def initialize(control, configuration)
        super
        @primary_key = configuration[:primary_key] || :id
        @target = configuration[:target] || raise(ETL::ControlError, "target must be specified")
        @table = configuration[:table] || raise(ETL::ControlError, "table must be specified")
        @columns = configuration[:columns]
        @latest_version = configuration[:latest_version] || :latest_version
      end
      
      # Process the row
      def process(row)
        conn = ETL::Engine.connection(target)
        primary_value = row[primary_key]
        q = "SELECT * FROM #{table_name} WHERE #{primary_key} = #{primary_value} AND #{latest_version} = 1 LIMIT 1"
        result = conn.select_one(q)
        return nil if result.nil?
       
        if columns.all?{|column| row[column].to_s == result[column.to_s].to_s}
          return row
        end
      end
      
private

      def table_name
        ETL::Engine.table(table, ETL::Engine.connection(target))
      end
    end
  end
end