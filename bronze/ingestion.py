
import lib.utils as utils
from lib.ingestors import IngestorFull, IngestorCDC

CATALOG = 'bronze'
DATABASE = 'DATABASE'
TABLE = 'TABLE'
FILE_FORMAT = 'csv'
PK_FIELD = 'FIELD'
UPDATE_FIELD = 'UPDATE_FIELD'

ingest_full = IngestorFull(
                            spark=spark
                            , catalog = CATALOG
                            , database= DATABASE
                            , table= TABLE
                            , file_format= FILE_FORMAT)
                            

ingest_cdc = IngestorCDC(
                            spark=spark
                            , catalog = CATALOG
                            , database= DATABASE
                            , table= TABLE
                            , file_format= FILE_FORMAT
                            , pk_field= PK_FIELD
                            , update_field= UPDATE_FIELD)
                            
if not utils.check_tables(spark, CATALOG, DATABASE, TABLE):
    print(f"Creating table {CATALOG}.{DATABASE}.{TABLE}...")
    ingest_full.run()
    dbutils.fs.rm(ingest_cdc.checkpoint_path, True)
    
ingest_cdc.run()
