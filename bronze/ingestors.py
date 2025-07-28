import yaml
import delta
import datetime
from pyspark.sql.types import StructField, StringType, DoubleType, LongType, StructType, TimestampType


class Ingestor:
    def __init__(self, spark, catalog, schema, table_name, input_format):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema        
        self.table_name = table_name
        self.input_format = input_format
        self.path = f'/Volumes/raw/{schema}/{table_name}'
        self.query_path = f'./{table_name}/{table_name}.sql'
        self._set_schema()
    
    def _set_schema(self):
        type_map = {
            'string': StringType(),
            'double': DoubleType(),
            'long': LongType(),
            'timestamp': TimestampType()
        }
        with open(f'{self.table_name}/{self.table_name}.yml', 'r') as f:
            schema_yml = yaml.safe_load(f)
            fields = [
                StructField(field['name'], type_map[field['type']], field['nullable']) for field in schema_yml['schema']
            ]   
        self.table_schema = StructType(fields)
        return self.schema

    def load(self):
        df = self.spark.read.format(self.input_format).schema(self.table_schema).load(f'{self.path}/*/*.json')
        df.createOrReplaceTempView(f'view_{self.table_name}')
        return f'view_{self.table_name}'

    def transform(self):
        with open(self.query_path, 'r') as f:
            query = f.read()
        df = self.spark.sql(query)
        return df
    
    def save(self, df):
        df.write.format('delta').mode('overwrite').saveAsTable(f'{self.catalog}.{self.schema}.{self.table_name}')
        return True
    
    def execute(self):
        df = self.load()
        transformed_df = self.transform()
        return self.save(transformed_df)

# class IngestorCDC(Ingestor):
#     def __init__(self, spark, catalog, schema, table_name, id_field, ts_field):
#         super().init(spark, catalog, schema, table_name)
#         self.id_field = id_field
#         self.ts_field = ts_field
#         self.set_delta()

#     def _set_delta(self):
#         table = f'{self.catalog}.{self.schema}.{self.table_name}'
#         self.delta_table = delta.DeltaTable.forName(self.spark, table_name)
    
#     def upsert(self, df):
#         df.createOrreplaceTempView(f'view_{self.table_name}')
#         query = f'''
#             SELECT * FROM global_temp.view_{self.table_name}
#             QUALIFY ROW_NUMBER() OVER (PARTITION BY {self.id_field} ORDER BY {self.timestamp_field}DESC) = 1
#             '''
#         df = self.spark.sql(query)

#         (self.deltatable.alias('old')
#             .merge(df.alias('new'), f'old.{self.id_field}'=f'new.{self.id_field}')
#             .whenMatchedDelete(condition = 'new.OP = "D"')
#             .whenMatchedUpdateAll(condition = 'new.OP = "U"')
#             .whenNotMatchedInsertAll(condition = 'new.OP = "I" OR new.OP = "U"')
#             .execute())
    
#     def load(self, path):
#         df = self.spark.readStream.format


    
    
    
