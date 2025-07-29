import yaml
import delta
import datetime
from pyspark.sql.types import StructField, StringType, DoubleType, LongType, StructType, TimestampType, StructType, ArrayType, IntegerType


class Ingestor:
    def __init__(self, spark, catalog, schema, table_name, input_format):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema        
        self.table_name = table_name
        self.input_format = input_format
        self.path = f'/Volumes/raw/{schema}/{table_name}'
        self.query_path = f'./{table_name}/{table_name}.sql'
    

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

        with open(f'{self.table_name}/{self.table_name}.yml', 'r') as f:
            schema_yml = yaml.safe_load(f)
            fields = [parse_field(field) for field in schema_yml['schema'] if 'name' in field]

        self.table_schema = StructType(fields)
        return self.table_schema


    def load(self):
        self._set_schema()
        df = self.spark.read.format(self.input_format).schema(self.table_schema).load(f'{self.path}/*.json')
        df.createOrReplaceTempView(f'view_{self.table_name}')
        return f'view_{self.table_name}'

    def transform(self):
        with open(self.query_path, 'r') as f:
            query = f.read()
        df = self.spark.sql(query)
        return df
    
    def save(self, df):
        (df.write
            .format('delta')
            .mode('overwrite')
            .saveAsTable(f'{self.catalog}.{self.schema}.{self.table_name}')
            )
        return True
    
    def run(self):
        df = self.load()
        transformed_df = self.transform()
        return self.save(transformed_df)

class IngestorCDC(Ingestor):
    def __init__(self, spark, catalog, schema, table_name, input_format, id_field, ts_field):
        super().__init__(spark, catalog, schema, table_name, input_format)
        self.id_field = id_field
        self.ts_field = ts_field
        self._set_delta()

    def _set_delta(self):
        table = f'{self.catalog}.{self.schema}.{self.table_name}'
        self.delta_table = delta.DeltaTable.forName(self.spark, table)
    
    def upsert(self, df):
        df.createOrReplaceTempView (f'view_{self.table_name}')
        query = f'''
            SELECT * FROM view_{self.table_name}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY {self.id_field} ORDER BY {self.ts_field} DESC) = 1
            '''
        df = self.spark.sql(query)

        (self.delta_table.alias('old')
            .merge(df.alias('new'), f'old.{self.id_field} = new.{self.id_field} and new.{self.ts_field} = old.{self.ts_field}')
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
    
    def load(self):
        self._set_schema()
        df = self.spark.read.format(self.input_format).schema(self.table_schema).load(self.path)
        return df

    def run(self):
        df = self.load()
        self.upsert(df)
        return True


    
    
    
