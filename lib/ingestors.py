import yaml
import delta
import datetime
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructField, StringType, DoubleType, LongType, StructType, TimestampType, StructType, ArrayType, IntegerType


class Ingestor:
    def __init__(self, spark, catalog, schema, table_name, input_format):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema        
        self.table_name = table_name
        self.input_format = input_format
        self.path = f'/Volumes/raw/{schema}/{self.table_name}'
        self.query_path = f'./{self.table_name}/{self.table_name}.sql'
    
    def _open_query(self):
        try:
            with open(self.query_path, 'r') as f:
                query = f.read()
            return query
        except Exception as e:
            print(f'Error opening query file: {e}')


    def _open_yml(self):
        try:
            with open(f'{self.table_name}/{self.table_name}.yml', 'r') as f:
                schema_yml = yaml.safe_load(f)
                return schema_yml.get('schema', [])
        except Exception as e:
            print(f'Error opening yml file: {e}')
    
    def _set_fields(self):
        schema_yml = self._open_yml()
        self.id_field = [f['name'] for f in schema_yml if f.get('key') == True][0]
        self.ts_field = [f['name'] for f in schema_yml if f.get('date_predicate') == True][0]
        return self.id_field, self.ts_field

    def _set_schema(self):
        def parse_field(field_def):
            type_name = field_def['type']
            nullable = field_def.get('nullable', True)

            if type_name == 'struct':
                sub_fields = [parse_field(f) for f in field_def['fields']]
                return StructField(field_def['name'], StructType(sub_fields), nullable)

            elif type_name == 'array':
                element_type_def = field_def.get('element_type')
                if element_type_def:
                    element_type = parse_field({'name': '', **element_type_def}).dataType
                else:
                    element_type = StringType() 
                return StructField(field_def['name'], ArrayType(element_type), nullable)

            else:
                type_map = {
                    'string': StringType(),
                    'double': DoubleType(),
                    'long': LongType(),
                    'integer': IntegerType(),
                    'timestamp': TimestampType()
                }
                return StructField(field_def['name'], type_map[type_name], nullable)
                    
        schema_yml = self._open_yml()
        try:
            fields = [parse_field(field) for field in schema_yml if 'name' in field]
            self.table_schema = StructType(fields)
        except Exception as e:
            print(f'Error setting schema: {e}')
                
    def load(self):
        self._set_schema()
        try:
            if self.input_format in ['json', 'parquet']:
                df = self.spark.read.format(self.input_format).schema(self.table_schema).load(f'{self.path}/*.{self.input_format}')
                df = df.withColumn('loaded_at', current_timestamp())
                df.createOrReplaceTempView(f'view_{self.table_name}')
            else:
                query = self._open_query()
                df = self.spark.sql(query)
                df.createOrReplaceTempView(f'view_{self.table_name}')
            return df
        except Exception as e:
            print(f'Error loading data: {e}')     

    def save(self, df):
        try:
            (df.write
                .format('delta')
                .mode('overwrite')
                .saveAsTable(f'{self.catalog}.{self.schema}.{self.table_name}')
            )
        except Exception as e:
            print(f'Error saving {self.table_name}: {e}')
        return True
    
    def run(self):
        print(f'Loading {self.table_name}')
        df = self.load()
        print(f'Saving {self.table_name} into {self.catalog}.{self.schema}.{self.table_name}')
        return self.save(df)

class IngestorCDC(Ingestor):
    def __init__(self, spark, catalog, schema, table_name, input_format):
        super().__init__(spark, catalog, schema, table_name , input_format)
        self._set_delta()
        self._set_fields()

    def _set_delta(self):
        table = f'{self.catalog}.{self.schema}.{self.table_name}'
        self.delta_table = delta.DeltaTable.forName(self.spark, table)
    
    def upsert(self, df):
        df.createOrReplaceTempView (f'view_{self.table_name}')
        query = self._open_query() + f'QUALIFY ROW_NUMBER() OVER (PARTITION BY {self.id_field} ORDER BY {self.ts_field} DESC) = 1'
        try:
            df = self.spark.sql(query)
            merge = (self.delta_table.alias('old')
                .merge(df.alias('new'), f'old.{self.id_field} = new.{self.id_field} and new.{self.ts_field} = old.{self.ts_field}')
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute())
            merge.display()
        except Exception as e:
            print(f'Error upserting {self.table_name}: {e}')
    
    def run(self):
        print(f'Loading {self.table_name}')
        df = self.load()
        print(f'Upserting {self.table_name}')
        self.upsert(df)
        print(f'Finished')
        return True


    
    
    
