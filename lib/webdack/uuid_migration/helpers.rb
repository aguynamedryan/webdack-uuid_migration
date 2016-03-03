module Webdack
  module UUIDMigration
    module Helpers

      # Converts primary key from Serial Integer to UUID, migrates all data by left padding with 0's
      #   sets uuid_generate_v4() as default for the column
      #
      # @param table [Symbol]
      # @param options [hash]
      # @option options [Symbol] :primary_key if not supplied queries the schema (should work most of the times)
      # @option options [String] :default mechanism to generate UUID for new records, default uuid_generate_v4(),
      #           which is Rails 4.0.0 default as well
      # @return [none]
      def primary_key_to_uuid(table, options={})
        options[:default] ||= 'uuid_generate_v4()'

        column = pk_column(table)

        column = copy_column(table, column, options)

        execute %Q{DROP SEQUENCE IF EXISTS #{table}_#{column}_seq} rescue nil
      end

      # Converts a column to UUID, migrates all data by left padding with 0's
      #
      # @param table [Symbol]
      # @param column [Symbol]
      #
      # @return [none]
      def column_to_uuid(table, column)
        fk_table = column.to_s.sub(/_id$/, '').pluralize

        return unless connection.table_exists?(fk_table.to_sym)

        orig_column = copy_column(table, column, default: nil)

        execute(%Q{
          UPDATE #{table} t SET #{column} = f.#{pk_column(fk_table)} FROM #{fk_table} f
            WHERE f.#{orig_pk_column(fk_table)} = t.#{orig_column}
        })
      end

      # Converts columns to UUID, migrates all data by left padding with 0's
      #
      # @param table [Symbol]
      # @param columns
      #
      # @return [none]
      def columns_to_uuid(table, *columns)
        columns.each do |column|
          column_to_uuid(table, column)
        end
      end

      def polymorphic_column_to_uuid(table, id_column, type_column, fk_tables)
        orig_column = copy_column(table, id_column, default: nil)
        fk_tables.each do |type_name, fk_table|
          execute(%Q{
            UPDATE #{table} t SET #{id_column} = f.#{pk_column(fk_table)} FROM #{fk_table} f
              WHERE f.#{orig_pk_column(fk_table)} = t.#{orig_column}
              AND t.#{type_column} = '#{type_name}'
          })
        end
      end

      def polymorphic_columns_to_uuid(table, columns = {})
        columns.each do |id_col, type_col|
          polymorphic_column_to_uuid(table, id_col, type_col, find_tables(table, type_col))
        end
      end

      def find_tables(table, type_col)
        types = select_values("SELECT #{type_col} FROM #{table} GROUP BY #{type_col} ORDER BY #{type_col}")
        types_and_tables = types.map do |a_type|
          [a_type, a_type.underscore.pluralize]
        end
        Hash[types_and_tables]
      end

      # Convert an Integer to UUID formatted string by left padding with 0's
      #
      # @param num [Integer]
      # @return [String]
      def int_to_uuid(num)
        '00000000-0000-0000-0000-%012d' % num.to_i
      end

      # Convert data values to UUID format for polymorphic associations. Useful when only few
      # of associated entities have switched to UUID primary keys. Before calling this ensure that
      # the corresponding column_id has been changed to :string (VARCHAR(36) or larger)
      #
      # See Polymorphic References in {file:README.md}
      #
      # @param table[Symbol]
      # @param column [Symbol] it will change data in corresponding <column>_id
      # @param entities [String] data referring these entities will be converted
      def polymorphic_column_data_for_uuid(table, column, *entities)
        list_of_entities= entities.map{|e| "'#{e}'"}.join(', ')
        execute %Q{
          UPDATE #{table} SET #{column}_id = #{to_uuid_pg("#{column}_id")}
            WHERE #{column}_type in (#{list_of_entities})
        }
      end

      def drop_transitory_columns
        droppers.uniq.each do |table, column|
          execute %Q{ALTER TABLE #{table} DROP COLUMN IF EXISTS #{column}}
        end
      end

      private
      # Prepare a fragment that can be used in SQL statements that converts teh data value
      # from integer, string, or UUID to valid UUID string as per Postgres guidelines
      #
      # @param column [Symbol]
      # @return [String]
      def to_uuid_pg(column)
        "uuid(lpad(replace(text(#{column}),'-',''), 32, '0'))"
      end

      def orig_pk_column(table)
        col = origify(pk_column(table))
        drop_column(table, col)
        col
      end

      def pk_column(table)
        connection.primary_key(table)
      end

      def droppers
        @droppers ||= []
      end

      def drop_column(table, column)
        droppers << [table, column]
      end

      def origify(column)
        column.to_s + "_orig"
      end

      def copy_column(table, column, options)
        default = options.fetch(:default, 'uuid_generate_v4()')

        orig_column = origify(column)

        execute %Q{
          ALTER TABLE #{table}
          ADD COLUMN #{orig_column} integer
        }

        execute %Q{
          UPDATE #{table}
          SET #{orig_column} = #{column}
        }

        execute %Q{
          ALTER TABLE #{table}
            ALTER COLUMN #{column} DROP DEFAULT,
            ALTER COLUMN #{column} SET DATA TYPE UUID USING (#{default_clause(default, column)}),
            ALTER COLUMN #{column} SET DEFAULT #{default.nil? ? 'NULL' : default}
         }

         drop_column(table, orig_column)

        return orig_column
      end

      def default_clause(default, column)
        default.nil? ? to_uuid_pg(column) : default
      end
    end
  end
end

ActiveRecord::Migration.class_eval do
  include Webdack::UUIDMigration::Helpers
end
