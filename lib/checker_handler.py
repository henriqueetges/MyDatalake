import logging
import time
import inspect
from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql import DataFrame as SparkDataFrame
from .checker import Checker
from .logging_utils import setup_logging

checker_handler, file_handler = setup_logging('checker_handler.log', 'checker_handler')

class CheckerHandler:
    def __init__(self, spark_session, dfs: dict):
      self.spark = spark_session
      self.dfs = dfs

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
    
      checker = Checker(self.spark, table_info)
      return checker.Annotate()

    def _select_standard_columns(self, df: SparkDataFrame, layer: str, table_name: str):
      """
      Selects the result column from the validations dataframe
      """
      return df.select(
          F.lit(layer).alias('layer'),
          F.lit(table_name).alias('table_name'),
          'df_key', 'test_type', 'column', 
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
          self._log_duration(f"Checks | {layer}.{table_name}", start)
          return self._select_standard_columns(annotated_df, layer, table_name)
      except Exception as e:
          checker_handler.error(f"Checks | {layer}.{table_name}: Failed to annotate {e}")
          checker_handler.error(str(e))
          return None

    def _compile_results(self, results: list[SparkDataFrame]) -> SparkDataFrame:
      """
      Union all the tables and their tests into a single dataframe
      """
      self._log_step("Compilation", "Compiling results")
      start = time.time()
      final_df = reduce(lambda df1, df2: df1.unionByName(df2), results)
      self._log_duration("Compilation", start)
      return final_df

    def run_checks(self):
      """
      Main function that serves as a handle for processing tables, adding results into 
      the result list and compling results.
      """
      results = []
      for table_info in self.dfs.values():
        try:
          result = self._process_table(table_info)
          if result is not None:
            results.append(result)
        except Exception as e:
          checker_handler.error(f'Compilation | Compilation failed because of {e}')
          return None
      checker_handler.removeHandler(file_handler)
      file_handler.close()
      return self._compile_results(results)
