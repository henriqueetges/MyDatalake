import json
import delta
from pyspark.sql.types import StructType


class Ingestors:
    """
    Class to handle the ingestion of data from various sources.
    """

    def __init__(self, spark, database, table, file_format, source, load_type):
        self.spark = spark
        self.database = database
        self.table = table
        self.file_format = file_format
        self.source = source
        self.load_type = load_type
        self.table_name = f"bronze.{self.database}.{self.table}"
        

class IngestorFull(Ingestors):
    """ Class to handle the ingestion of data from a full load source.

    Args:
        Ingestors (_type_):     _Parent class for all ingestors
    """
    
    def __init__(self, spark, database, table, file_format, source):
        super().__init__(self, spark, database, table, file_format, source, load_type = 'full_load')
        self.path = f"Data/raw/{self.load_type}/{self.source}/{self.database}/{self.table}/"    
        
    def load(self):
        """Loads data from the specified path and file format.

        Returns:
            _type_: DataFrame
        """
        df = (self.spark.read.format(self.file_format).load(self.path))
        return df
    
    def save(self, df, mode='overwrite'):
        """Saves the DataFrame to a Delta table.

        Args:
            df (_type_): Dataframe
            mode (str, optional): Mode of saving into the database. Defaults to 'overwrite'.
        """
        (df.write
            .format("delta")
            .mode(mode)
            .option("overwriteSchema", "true")
            .saveAsTable(f"{self.database}.{self.table}"))
    
    def run(self):
        df = self.load
        self.save(df)
        

class IngestorCDC(Ingestors):
    """
    Class to handle the ingestion of data from a CDC source.
    """

    def __init__(self, spark, database, table, file_format, pk_field, update_field):
        """Initializes the IngestorCDC class.

        Args:
            spark (_type_): Spark session
            database (_type_):  _database name
            table (_type_): _table name
            file_format (_type_):  _file format (e.g., 'json', 'parquet')
            pk_field (_type_): _primary key field
            update_field (_type_): _update field (e.g., 'updated_at')
        """
        super().__init__(spark, database, table, file_format,load_type='cdc')
        self.pk_field = pk_field
        self.update_field = update_field
        self.path = f"Data/raw/{self.load_type}/{self.source}/{self.database}/{self.table}/"
            
    def transform(self, df):
        """Transforms the DataFrame to handle CDC events.

        Args:
            df (_type_): DataFrame

        Returns:
            _type_: Transformed DataFrame
        """
        df.createOrReplaceTempView(f"cdc.{self.database}_{self.table}")
        query = f"""
            SELECT * FROM cdc.{self.database}_{self.table}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY {self.pk_field} ORDER BY {self.update_field} DESC) = 1
        """
        # Transform the DataFrame to handle CDC events
        return self.spark.sql(query)
    
    def save(self, df):
        bronze_data = delta.DeltaTable.forName(self.spark, f"{self.database}.{self.table}")
        (bronze_data
            .alias("old")
            .merge(df.alias("new"),f"old.{self.pk_field} = new.{self.pk_field}")
            .whenMatchedDelete(condition = "new.Operation = 'D' AND old.{self.update_field} < new.{self.update_field}")
            .whenMatchedUpdateAll(condition = f"new.Operation = 'U' AND old.{self.update_field} < new.{self.update_field}")
            .whenNotMatchedInsertAll()
            .execute())
    
    def run(self):
        df = self.load()
        transformed_df = self.transform(df)
        self.save(transformed_df)
        
class IngestorCDCStreaming(Ingestors):

    def __init__(self, spark, database, table, file_format, source, table_name,pk_field, update_field):
        """Initializes the IngestorCDCStreaming class."""
        super().__init__(spark, database, table, file_format, source, table_name, load_type="cdc_streaming")
        self.pk_field = pk_field
        self.update_field = update_field
        self.path = f"Data/raw/{self.load_type}/{self.source}/{self.database}/{self.table}/"
        self.checkpoint = f"Data/raw/{self.load_type}/{self.source}/{self.database}/checkpoint_{self.table}/"
        self.tablename = f"bronze.{self.database}.{self.table}"
        self.schema = self.load_schema()        
        
    def load_schema(self):
        """Loads the schema from the Delta table.

        Returns:
            _type_: StructType
        """
        path = f"{self.database}/{self.table}/schema.json"
        with open(path, "r") as f:
            schema = json.load(f)
        return StructType.fromJson(schema)
    
    def load(self):
        """Loads data from the specified path and file format.

        Returns:
            _type_: DataFrame
        """
        df = (self.spark.readStream.format(self.file_format)
            .schema(self.schema)
            .load(self.path))
        return df
    
    def upsert(self, df, bronze_df):
        """ Performs upsert operation on the DataFrame.

        Args:
            df (DataFrame): DataFrame to be upserted
            bronze_df (DataFrame): Bronze DataFrame to be merged with
        """
        df.createOrReplaceGlobalTempView(f"bronze.{self.database}_{self.table}_bronze_cdc")
        query = f"""
            SELECT * FROM global_temp.{self.table}_bronze_cdc 
            QUALIFY ROW_NUMBER() OVER (PARTITION BY {self.pk_field} ORDER BY {self.update_field} DESC) = 1
        """        
        df_unique = self.spark.sql(query)
        (bronze_df.alias("old")
            .merge(df_unique.alias("new"), f"old.{self.pk_field} = new.{self.pk_field}")
            .whenMatchedDelete(condition = "new.Operation = 'D' AND old.{self.update_field} < new.{self.update_field}")
            .whenMatchedUpdateAll(condition = f" new.Operation = 'U' AND old.{self.update_field} < new.{self.update_field}")
            .whenNotMatchedInsertAll()
            .execute())
    
    def save(self, df):
        """Saves the DataFrame to a Delta table using streaming

        Args:
            df (DataFrame): DataFrame to be saved

        Returns:
            _type_: StreamingQuery
        """
        bronze = delta.DeltaTable.forName(self.spark, self.tablename)
        stream = (df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", self.checkpoint)
            .foreachBatch(lambda df, _: self.upsert(df, bronze))
            .trigger(availableNow=True))
        return stream
    
    def run(self):
        """_summary__
        Loads the data, transforms it, and saves it to the Delta table.

        Returns:
            _type_: StreamingQuery
        """
        df = self.load()
        stream = self.save(df)
        return stream.start()