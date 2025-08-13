import traceback
import time
import inspect
from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql import DataFrame as SparkDataFrame
from .checker import Checker
from .logging_utils import setup_logging
from delta.tables import DeltaTable

checker_handler, file_handler = setup_logging('checker_handler.log', 'checker_handler')

class CheckerHandler:
    def __init__(self, spark_session, dfs: dict):
      self.spark = spark_session
      self.dfs = dfs
      self.results = []

    def _log_step(self, step: str, message: str) -> None:
      """
      Logs the steps performed

      Parameters:
        step: str
        message: str
      """
      caller = inspect.stack()[1].function
      checker_handler.info({'step': step, 'caller': caller, 'message': message})

    def _log_duration(self, step: str, start_time: float) -> float:
      """
      Logs the duration of the step

      Parameters:
        step: 
        start_time:
      """
      duration = time.time() - start_time
      self._log_step(step, f"completed in {duration:.2f} seconds")
      return duration

    def _annotate_dataframe(self, table_info: dict) -> SparkDataFrame :
      """
      Instantiates the Checker class so that it can run 
      the checks against its columns

      Parameters:
        df
        metadata_path:
      """
      try:
        checker = Checker(self.spark, table_info)
      except Exception as e:
        checker_handler.error(f"Checks | {table_info.get('table_name')}: Failed to instantiate checker {e}")
      return checker.Annotate()

    def _select_standard_columns(self, df: SparkDataFrame, layer: str, table_name: str):
      """
      Selects the result column from the validations dataframe
      """
      return df.select(
          F.lit(layer).alias('layer'),
          F.lit(table_name).alias('table_name'),
          'df_key', 'test_type', 'test_name', 'column', 'mandate',
          'run_date', 'check_result', 'check_score'
      )

    def _process_table(self, table_info: dict) -> SparkDataFrame:
      """
      Processes the table by fetching its metadata, calling the annotation method
      to perform checks and selecting the result columns
      """      
      table_name = table_info.get('table_name')
      layer = table_info.get('catalog')
      metadata_path = table_info.get('metadata_path')

      self._log_step("Metadata", f"Fetching metadata for {table_name}")
      self._log_step("Metadata", f"{table_name} metadata fetched")
      self._log_step("Checks", f"Checking {layer}.{table_name} using info from {metadata_path}")
      try:
          start = time.time()
          annotated_df = self._annotate_dataframe(table_info)
          if annotated_df:
            self._log_duration(f"Checks | {layer}.{table_name}", start)
            standardized_df = self._select_standard_columns(annotated_df, layer, table_name)
            self.results.append(standardized_df)
      except Exception as e:
        error_type = type(e).__name__
        error_msg = str(e)
        tb = traceback.format_exc()      
        checker_handler.error({
          'step': 'Checks',
          'table': f'{layer}.{table_name}',
          'error_type': error_type,
          'error_msg': error_msg,
          'traceback': tb
        })
          
        return None

    def _compile_results(self) -> SparkDataFrame:
      """
      Union all the tables and their tests into a single dataframe
      """
      self._log_step("Compilation", "Compiling results")
      start = time.time()
      if not self.results:
        checker_handler.error('No Results to compiled')
        return None
      final_df = reduce(lambda df1, df2: df1.unionByName(df2), self.results)
      final_df.createOrReplaceTempView('silver.checks.tempv_column_checks')
      self._log_duration("Compilation", start)
      return final_df
              
    
    def _save_checks(self, df: SparkDataFrame):
      self._log_step('Saving', 'Saving results')
      try:
        
        df = df.withColumn("run_date", F.col("run_date").cast("date")) \
          .withColumn("check_score", F.col("check_score").cast("double"))

        target_table = 'silver.checks.column_checks'
        delta_table = DeltaTable.forName(self.spark, target_table)

        delta_table.alias('target') \
          .merge(
            source=df.alias('source'),
            condition="""
              target.table_name = source.table_name 
              AND target.column = source.column
              AND target.test_type = source.test_type
              AND target.layer = source.layer
            """
          ).whenMatchedUpdateAll() \
          .whenNotMatchedInsertAll() \
          .execute()
      except Exception as e:
        raise RuntimeError(f"Failed to save checks: {e}")


    def run_checks(self):
      """
      Main function that serves as a handle for processing tables, adding results into 
      the result list and compling results.
      """ 
      self.results = []     
      for table_info in self.dfs.values():
        self._process_table(table_info)
      final_df = self._compile_results()
      if final_df:
        self._save_checks(final_df)
      checker_handler.removeHandler(file_handler)
      file_handler.close()
      return final_df
