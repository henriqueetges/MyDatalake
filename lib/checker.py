import yaml
import datetime
import pyspark.sql.functions as F
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from .logging_utils import setup_logging
import time
import logging
import pyspark.sql.types as SType


checker, file_handler = setup_logging('checker.log', 'checker')

class Checker:
    def __init__(self, spark_session: SparkSession, table_instructions: dict):
        self.spark_session = spark_session
        self.table_instructions = table_instructions
        self._parse_instructions()
    
    def _parse_instructions(self):
      self.metadata_path = self.table_instructions.get('metadata_path')
      self.df = self.table_instructions.get('dataframe')
      self.table_name = self.table_instructions.get('table_name')
      self.schema = self.table_instructions.get('schema')
      self.catalog = self.table_instructions.get('catalog')
      with open(self.metadata_path, 'r') as f:
        self.table_config = yaml.safe_load(f)


    def _log_step(self, step: str, message: str) -> None:
      """
      Logs the steps performed

      Parameters:
        step: str
        message: str
      """
      checker.info({'step': step, 'message': message})
    
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
    
    def get_column_tests(self) -> list[dict]:
      """
      Parses the yaml file and returns a list of key values which are
      easier to use when performing the checks

      Parameters:
        None
      """

      self._log_step('test_fetch', 'Getting tests')
      test_cols = []
      self.df_key = None

      for col in self.table_config.get('schema', []):
        col_name = col.get('name')
        if col.get('key') is True and self.df_key is None:
          self.df_key = col_name

        for test in col.get('tests', []):
          test_cols.append({
              'column': col_name,
              'type': col.get('type'),
              'mandate': col.get('mandate'),
              'test_type': test.get('test_type', ''),
              'test_name': test.get('test_name', ''),
              'kwargs': test.get('kwargs', {})
            })

      if not self.df_key:
        raise ValueError("No key column found in schema.")

      if self.df_key not in self.df.columns:
        raise ValueError(f"Key column '{self.df_key}' not found in DataFrame.")

      self.df = self.df.withColumn('df_key', F.col(self.df_key))

      self._log_step('test_fetch', 'Getting tests finished')
      return test_cols

    
    def _build_result(self, result_col, score, **kwargs) -> SparkDataFrame:
      """
      BUilds the result dataframe, based on the tests performed against each of the columns

      Parameters:
        result_col: str
        score: str
        kwargs: dict
      """
      self._log_step('Resultset', 'Building resultset')
      column_name = kwargs.get('column', '')
      test_type = kwargs.get('test_type', '')   
      mandate = kwargs.get('mandate', '')
      results = self.df.select(
        'df_key', 
        F.lit(test_type).alias('test_type'), 
        F.lit(mandate).alias('mandate'),
        F.lit(column_name).alias('column'),
        F.lit(datetime.date.today()).alias('run_date'),
        result_col.alias('check_result'),
        score.alias('check_score')
      )
      self._log_step('Resultset', 'Building resultset finished')
      return results
        
    def Annotate_missing(self, **kwargs) -> SparkDataFrame: 
      """
      Checks a column for missing value, stores results in a dataframe.

      Parameters:
        kwargs: dict
      """
      column_name = kwargs.get('column', '')
      if not column_name:
        raise ValueError(f'{column_name} not found')
      self._log_step('Tests', f'Checking {column_name} for nulls using {self.df_key}')
      test = ((F.col(column_name).isNull()) | (F.col(column_name).cast('string') == '0.0'))
      result = F.when(test, 'failed').otherwise(F.lit('passed'))
      score = F.when(test, '0').otherwise(F.lit('1'))
      self._log_step('Tests', f'Checking {column_name} for nulls finished')
      return self._build_result(result, score,**kwargs)

    def Annotate_duplicated(self, **kwargs) -> SparkDataFrame:
      """
      Checks a column for duplicated values, stores results in a dataframe

      Parameters:
        kwargs: dict 
      """
      column_name = kwargs.get('column', '')
      if not column_name:
        raise ValueError(f'{column_name} not found')
      self._log_step('Tests', f'Checking {column_name} for duplicates')
      window = Window.partitionBy(column_name).orderBy(F.lit(1))
      self.df = self.df.withColumn("row_number_window", F.row_number().over(window))
      test = (F.col("row_number_window") > 1)
      result = F.when(test, 'failed').otherwise(F.lit('passed'))
      score = F.when(test, '0').otherwise(F.lit('1'))
      result = self._build_result(result, score, **kwargs)
      self.df = self.df.drop("row_number_window")
      self._log_step('Tests', f'Checking {column_name} for duplicates finished')
      return result

    def Annotate_outdated(self, **kwargs) -> SparkDataFrame:
      """
      Checks a column for outdated records, stores results in a dataframe

      Parameters:
        kwargs: dict

      THIS NEEDS TO BE REFACTORED TO CHECK FOR VALUE IN RANGE 
      """
      
      column_name = kwargs.get('column', '')
      if not column_name:
        raise ValueError(f'{column_name} not found')
      self._log_step('Tests', f'Checking {column_name} for timeliness')
      threshold = kwargs.get('kwargs', {}).get('threshold', 0)    
      test = (F.col(column_name) < F.date_sub(F.current_date(), threshold))
      # change scoring here based on the degree of change to the expected date
      result = F.when(test, 'failed').otherwise(F.lit('passed'))
      score = F.when(test, '0').otherwise(F.lit('1'))
      self._log_step('Tests', f'Checking {column_name} for timeliness finished')  
      return self._build_result(result, score,**kwargs)


    def Annotate_not_rules(self, **kwargs) -> SparkDataFrame:
      """
      Checks if a column matched an expression criteria, stores results in a dataframe.

      Parameters:
        kwargs: dict
      """
      
      column_name = kwargs.get('column') 
      if not column_name:
        raise ValueError(f'{column_name} not found')      
      expression = kwargs.get('kwargs', {}).get('expression', {})
      self._log_step('Tests', f'Checking {column_name} for validity using {expression}')
      test = (F.when(F.col(column_name).isNull(), F.lit(True)).otherwise(F.expr(expression)))
      result = F.when(test, 'failed').otherwise(F.lit('passed'))
      score = F.when(test, '0').otherwise(F.lit('1'))
      self._log_step('Tests', f'Checking {column_name} for validity finished')
      return self._build_result(result, score,**kwargs)
      

    def Annotate_not_in_list(self, **kwargs) -> SparkDataFrame:
      """
      Checks if a column's values are within expected values, stores results in a dataframe.

      parameters:
        kwargs: dict
      """
      
      column_name = kwargs.get('column')      
      if not column_name:
        raise ValueError(f'{column_name} not found')
      self._log_step('Tests', f'Checking {column_name} for unexpected values')
      expected_values = kwargs.get('kwargs',{}).get('expected_values', [])
      test = ~(F.lower(F.trim(F.col(column_name))).isin([v.lower() for v in expected_values]))
      result = F.when(test, 'failed').otherwise(F.lit('passed'))
      score = F.when(test, '0').otherwise(F.lit('1'))
      self._log_step('Tests', f'Checking {column_name} for unexpected values finished')
      return self._build_result(result, score,**kwargs)

    def Annotate_pattern_inconsistency(self, **kwargs):
      """
      Checks if a column doesn't follow a reget pattern, stores results in a dataframe

      Parameters:
        kwargs: dict
      """
      
      column_name, pattern = kwargs.get('column'), kwargs.get('pattern')
      if not column_name:
        raise ValueError(f'{column_name} not found')
      self._log_step('Tests', f'Checking {column_name} for pattern {pattern}')
      if column_name is None or pattern is None:
        raise ValueError('Unespecified column or pattern')
      test = (F.when(~F.col(column_name).rlike(pattern), True).otherwise(False))
      result = F.when(test, 'failed').otherwise(F.lit('passed'))
      score = F.when(test, '0').otherwise(F.lit('1'))
      self._log_step('Tests', f'Checking {column_name} for pattern finished')
      return self._build_result(result, score,**kwargs)

    
    def Annotate_type_inconsistency(self, **kwargs):
      """
      Checks if a column doesn't follow the specified type, stores results in a Dataframe

      Parameters:
        kwargs: dict
      """
      
      column_name, expected_type = kwargs.get('column'), kwargs.get('type')
      if not column_name:
        raise ValueError(f'{column_name} not found')
      self._log_step('Tests', f'Checking {column_name} type consistency for {expected_type}')
      if column_name is None or expected_type is None:
        raise ValueError('Unespecified column or column type')
      casted = F.col(column_name).cast(expected_type)      
      test = casted.isNull() & F.col(column_name).isNotNull()  
      result = F.when(test, 'failed').otherwise(F.lit('passed'))
      score = F.when(test, '0').otherwise(F.lit('1'))
      self._log_step('Tests', f'Checking {column_name} for type consistency finished')
      return self._build_result(result, score,**kwargs)

      
     

    # def Annotate_not_consistent_with(self, column_name: str, reference_df: SparkDataFrame,**kwargs)-> SparkDataFrame:      
    #   for col_info in kwargs['column_names']:
    #     column_name = col_info['name']
    #     reference_column = col_info['reference_column']
    #     mapped_table = col_info['mapped_table']
    #     common_key = col_info['common_key']
    #     self.df = self.df.alias('source')
    #     reference_df = self.spark_session.read.parquet(mapped_table, header=True, sep=';', inferSchema=True).select(common_key, reference_column).alias('reference')
    #     self.df = self.df.join(reference_df, on=self.df[f'source.{common_key}'] == reference_df[f'reference.{common_key}'], how="left")
    #     self.df = (self.df.withColumn(f"{column_name}_not_consistent_with"
    #       , F.col(f'source.{column_name}') != F.col(f'reference.{reference_column}')))
    #     self.df = self.df.drop(f'reference.{reference_column}')
    #   return self.df

    def Annotate(self) -> SparkDataFrame:
      """
      Mapping function that takes the column metadata dictionary and
      calls the appropriate function on the column depending on the 
      tests assigned to it. 

      Aggregates the individual dataframes into a resulting dataframe 
      with all checks
      """
      expectation_funcs = {
        'missing': self.Annotate_missing,
        'duplicated': self.Annotate_duplicated,
        'outdated': self.Annotate_outdated,
        'outside_of_rules': self.Annotate_not_rules,
        'not_in_list': self.Annotate_not_in_list,  
        'type_mismatch': self.Annotate_type_inconsistency,
        'pattern_mismatch':self.Annotate_pattern_inconsistency,
        #consistency': self.Annotate_not_consistent_with,      
     }
      dfs = []
      tests = self.get_column_tests()
      for test_params in tests:
        if test_params.get('test_type') not in expectation_funcs:
          raise ValueError(f'Unsupported expectation type: {test_params.get("test_type")}')
        else:
            self._log_step('Perfoming Checks', 'Started running checks')
            df = expectation_funcs[test_params.get('test_type')](**test_params)
            dfs.append(df)
            self._log_step('Perfoming Checks', 'Finished running checks')
      self._log_step('Aggregating results', 'Started')
      final_df = dfs[0]
      for df in dfs[1:] :        
        final_df = final_df.unionByName(df)        
      self._log_step('Aggregating results', 'Finished')
      checker.removeHandler(file_handler)
      file_handler.close()
      return final_df
            


    

           
          



