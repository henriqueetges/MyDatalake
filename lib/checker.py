import yaml
import datetime
import pyspark.sql.functions as F
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from .logging_utils import setup_logging
from functools import reduce
import time
import re
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
      try:
        with open(self.metadata_path, 'r') as f:
          self.table_config = yaml.safe_load(f)
      except FileNotFoundError:
        raise FileNotFoundError(f'Metadata file not found at path: {self.metadata_path}')
      except yaml.YAMLError as e:
        raise ValueError(f'Error parsing YAML file: {e}')
        



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
      schema = self.table_config.get('schema', [])
      if not isinstance(schema, list):
        raise ValueError("Schema must be a list of dictionaries.")

      test_cols = []
      key_cols = []
      self.df_key = None

      for col in schema:
        col_name = col.get('name')
        if col.get('key') is True:
          key_cols.append(col_name)          

        for test in col.get('tests', []):
          test_cols.append({
              'column': col_name,
              'type': col.get('type'),
              'mandate': col.get('mandate'),
              'test_type': test.get('test_type', ''),
              'test_name': test.get('test_name', ''),
              'kwargs': test.get('kwargs', {})
            })
        self._log_step('test_fetch', f'{col_name} has {len(col.get("tests", []))} tests')  

      if not key_cols:
        raise ValueError("No key column found in schema.")
   
      missing_keys = [c for c in key_cols if c not in self.df.columns]
      if missing_keys:
        raise ValueError(f"Key column not found in DataFrame.")
      composite_key = F.concat_ws('_', *[F.col(c).cast('string') for c in key_cols])
      
      self.df = self.df.withColumn('df_key', composite_key)
      self.df_key = 'df_key'
      self._log_step('test_fetch', f'Key created using {self.df_key}')
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
      test_name= kwargs.get('test_name', '')
      results = self.df.select(
        F.col(str(self.df_key)).alias('df_key'), 
        F.lit(test_type).alias('test_type'), 
        F.lit(mandate).alias('mandate'),
        F.lit(column_name).alias('column'),
        F.lit(test_name).alias('test_name'),
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
      self._log_step('Tests', f'Checking {column_name} for nulls using {self.df_key}')
      if not column_name:
        raise ValueError(f'Column not found')
      invalid_values = ['', '0.0']
      test = F.col(column_name).isNull() | F.col(column_name).cast('string').isin(invalid_values)
      result = F.when(test, 'failed').otherwise(F.lit('passed'))
      score = F.when(test, F.lit(0)).otherwise(F.lit(1))
      self._log_step('Tests', f'Checking {column_name} for nulls finished')
      return self._build_result(result, score,**kwargs)

    def Annotate_duplicated(self, **kwargs) -> SparkDataFrame:
      """
      Checks a column for duplicated values, stores results in a dataframe

      Parameters:
        kwargs: dict 
      """
      column_name = kwargs.get('column', '')
      self._log_step('Tests', f'Checking {column_name} for duplicates')
      if not column_name:
        raise ValueError(f'Column not found')      
      window = Window.partitionBy(column_name).orderBy(F.col(self.df_key))
      self.df = self.df.withColumn("row_number_window", F.row_number().over(window))
      test = (F.col("row_number_window") > 1)
      result = F.when(test, 'failed').otherwise(F.lit('passed'))
      score = F.when(test, F.lit(0)).otherwise(F.lit(1))
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
      threshold = kwargs.get('kwargs', {}).get('threshold', 0)    
      column_name = kwargs.get('column', '')
      if not column_name or threshold is None:
        raise ValueError(f'column or threshold not found in configs')
      self._log_step('Tests', f'Checking {column_name} for timeliness')
      test = (F.col(column_name) < F.date_sub(F.current_date(), threshold))
      # change scoring here based on the degree of change to the expected date
      result = F.when(test, 'failed').otherwise(F.lit('passed'))
      score = F.when(test, F.lit(0)).otherwise(F.lit(1))
      self._log_step('Tests', f'Checking {column_name} for timeliness finished')  
      return self._build_result(result, score,**kwargs)


    def Annotate_not_rules(self, **kwargs) -> SparkDataFrame:
      """
      Checks if a column matched an expression criteria, stores results in a dataframe.

      Parameters:
        kwargs: dict
      """
      expression = kwargs.get('kwargs', {}).get('expression', {})
      column_name = kwargs.get('column') 
      if not column_name or expression is None:
        raise ValueError(f'Mising column or expressin in configs')            
      self._log_step('Tests', f'Checking {column_name} for validity using {expression}')
      test = F.col(column_name).isNull() |~F.expr(expression)
      result = F.when(test, 'failed').otherwise(F.lit('passed'))
      score = F.when(test, F.lit(0)).otherwise(F.lit(1))
      self._log_step('Tests', f'Checking {column_name} for validity finished')
      return self._build_result(result, score,**kwargs)
      

    def Annotate_not_in_list(self, **kwargs) -> SparkDataFrame:
      """
      Checks if a column's values are within expected values, stores results in a dataframe.

      parameters:
        kwargs: dict
      """
      expected_values = kwargs.get('kwargs',{}).get('expected_values', [])
      column_name = kwargs.get('column')      
      if not column_name or expected_values is None:
        raise ValueError(f'column or expected values not found')
      self._log_step('Tests', f'Checking {column_name} for unexpected values')
      normalized_values = [v.lower() for v in expected_values]    
      test = ~(F.lower(F.trim(F.col(column_name))).isin([normalized_values]))
      result = F.when(test, 'failed').otherwise(F.lit('passed'))
      score = F.when(test, F.lit(0)).otherwise(F.lit(1))
      self._log_step('Tests', f'Checking {column_name} for unexpected values finished')
      return self._build_result(result, score,**kwargs)

    def Annotate_pattern_inconsistency(self, **kwargs):
      """
      Checks if a column doesn't follow a reget pattern, stores results in a dataframe

      Parameters:
        kwargs: dict
      """      
      column_name = kwargs.get('column')
      pattern = kwargs.get('pattern')
      if not column_name or pattern is None:
        raise ValueError(f'column or pattern not found')
      try: 
        re.compile(pattern)
      except re.error:
        raise ValueError(f'pattern {pattern} is not a valid regular expression')

      self._log_step('Tests', f'Checking {column_name} for pattern {pattern}')
      test = ~F.col(column_name).rlike(pattern)
      result = F.when(test, 'failed').otherwise(F.lit('passed'))
      score = F.when(test, F.lit(0)).otherwise(F.lit(1))
      self._log_step('Tests', f'Checking {column_name} for pattern finished')
      return self._build_result(result, score,**kwargs)

    
    def Annotate_type_inconsistency(self, **kwargs):
      """
      Checks if a column doesn't follow the specified type, stores results in a Dataframe

      Parameters:
        kwargs: dict
      """
      column_name = kwargs.get('column')
      expected_type = kwargs.get('type')
      if not column_name or not expected_type:
        raise ValueError(f'column or expected type not found')

      self._log_step('Tests', f'Checking {column_name} type consistency for {expected_type}')
      casted = F.col(column_name).cast(expected_type)      
      test = casted.isNull() & F.col(column_name).isNotNull()  
      result = F.when(test, 'failed').otherwise(F.lit('passed'))
      score = F.when(test, F.lit(0)).otherwise(F.lit(1))
      self._log_step('Tests', f'Checking {column_name} for type consistency finished')
      return self._build_result(result, score,**kwargs)

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
        
     }
      dfs = []
      tests = self.get_column_tests()
      for test_params in tests:
        test_type = test_params.get('test_type')
        if test_type not in expectation_funcs:
          raise ValueError(f'Unsupported expectation type: {test_type}')
        else:
            self._log_step('Perfoming Checks', 'Started running checks')
            df = expectation_funcs[test_type](**test_params)
            dfs.append(df)
            self._log_step('Perfoming Checks', 'Finished running checks')
      self._log_step('Aggregating results', 'Started')
      final_df = reduce(lambda df1, df2: df1.unionByName(df2), dfs)
      self._log_step('Aggregating results', 'Finished')
      checker.removeHandler(file_handler)
      file_handler.close()
      return final_df
            


    

           
          



