from dataflow.operators import (
   runcontext,
)
 
from dataflow_commons.operators import PrestoInsertOperatorWithSchema
from dataflow_commons.operators.schema import Column, HiveAnon, Table
from tasks.consumer_hardware.common.config import Config
 
retention_days=Config.get_retention_days(runcontext)
 
dq_ssot_audit_log_table = Table(
   cols=[
       Column('data', 'MAP(VARCHAR, VARCHAR)', """
           Key -> Value where key is the column name and value is the value of that column.
           This contains columns that are pertinent to the rule and also would uniquely idetify the
           record for later analysis.
           """
           , HiveAnon.NONE),
       Column('created_timestamp', 'TIMESTAMP', 'Time of execution', HiveAnon.NONE),
   ],
   partitions=[
       Column('ds', 'VARCHAR', 'ds'),
       Column('table_name', 'VARCHAR', 'Name of table affected by rule.'),
       Column('rule_name', 'VARCHAR' , 'Name of the rule that is reason for this insertion', HiveAnon.NONE),
   ],
   initial_retention=retention_days,
)
 
dq_ssot_audit_aggregate_log_table = Table(
   cols=[
       Column('data', 'MAP(VARCHAR, BIGINT)', """
           Key -> Value where key is the column name and value is the distinct count for that respective column
                  which is being aggregated for that day.
           """
           , HiveAnon.NONE),
       Column('created_timestamp', 'TIMESTAMP', 'Time of execution', HiveAnon.NONE),
   ],
   partitions=[
       Column('ds', 'VARCHAR', 'ds'),
       Column('table_name', 'VARCHAR', 'Name of table the distinct counts apply to.'),
   ],
   initial_retention=retention_days,
)
 
class Util(object):
 
   AUDIT_AGG_LOG_INSERT_SQL = """
       SELECT
           MAP(
               ARRAY[{col_names_expr}],
               ARRAY[{approx_distincts_expr}]
           ) AS data,
           CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS created_timestamp,
           '{ds}' AS ds,
           '{table_name}'
       FROM {table_name}
       WHERE
           ds = '{ds}'
   """
 
   # If the table has zero records in it (so before the first run)
   # Then the MixedSourceDqOperator Operator fails
   # So a dummmy 0 is returned to handle this case
   TARGET_QUERY_AGG_LOG_SQL = """
       WITH union_cte_for_empty_table AS (
           SELECT
               1 AS priority,
               data['{col_name}'] AS distinct_{col_name}
           FROM {dq_agg_table}
           WHERE
               ds = '{ds_minus_1}'
 
           UNION ALL
 
           SELECT
               0 AS priority,
               0
       )
 
       SELECT
           distinct_{col_name}
       FROM union_cte_for_empty_table
       ORDER BY
           priority DESC
       LIMIT
           1
   """
 
   SOURCE_QUERY_AGG_LOG_SQL = """
       SELECT
           APPROX_DISTINCT({col_name}) AS distinct_{col_name}
       FROM {table_name}
       WHERE
           ds = '{ds}'
   """
 
   USER_QUERY = """
       SELECT DISTINCT
           {col_name}
       FROM {table_name}
       WHERE
           ds = '{ds}'
 
       EXCEPT
 
       SELECT DISTINCT
           {col_name}
       FROM {table_name}
       WHERE
           ds = '{ds_minus_1}'
   """
 
   @classmethod
   def get_agg_target_select_args(cls, cols, table_name, dq_ds):
       """
           Return a map that can be used in the format method of the query string
 
       :param cols: An iterable of the cols being aggregated
       :param table_name: sets the table_name of the aggregate query
       :param dq_ds: sets the ds of the query
 
       """
 
       approx_distincts_expr = ",".join([f"APPROX_DISTINCT({col})" for col in cols])
       col_names_expr = ",".join([f"'{col}'" for col in cols])
 
       return {
           'approx_distincts_expr':approx_distincts_expr,
           'col_names_expr':col_names_expr,
           'table_name':table_name,
           'ds':dq_ds
       }
 
 
class DQBusinessCheckBuilder():
   """
   DQBusinessCheckBuilder encapsulates a PrestoInsertOperatorWithSchema to the
   dq_ssot_audit_log table
   """
   def __init__(self, dq_table='test_dq_ssot_audit_log',rule_name="test"):
       """
       Construct a new 'DQBusinessCheckBuilder' object.
 
       :param dq_table: sets the name of the audit log table which would likely be
           test_dq_ssot_audit_log or dq_ssot_audit_log
           The default is `test_dq_ssot_audit_log`
       :param rule_name: sets the name of the rule which is likely to be dependant on
           the logic.  It could also be 'test'. The default is `test`.
       """
       self.table_name=""
       self.rule_name=rule_name
       self.cols = []
       self.presto_insert_args = {}
       self.col_names_expr = ""
       self.cast_cols_as_varchar_expr = ""
       self.ds=""
       self.dq_table=dq_table
       self.AUDIT_LOG_SQL = self._set_audit_log_sql()
 
   def _set_audit_log_sql(self):
       """
       Return the SQL for insertion into dq_ssot_audit_log.
 
       """
       return """
           SELECT
               MAP(
                   ARRAY[{col_names_expr}],
                   ARRAY[{cast_cols_as_varchar_expr}]
               ) AS data,
               CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS created_timestamp,
               '{ds}' AS ds,
               '{table_name}' AS table_name,
               '{rule_name}' AS rule_name
           FROM {table_name} base_table
           WHERE
               ds = '{ds}'
               AND {filter_expr}
       """
 
   def _set_col_exprs(self):
       self.col_names_expr = ",".join([f"'{col}'" for col in self.cols])
       self.cast_cols_as_varchar_expr = ",".join([f"CAST({col} AS VARCHAR)" for col in self.cols])
 
   def set_rule_name(self, rule_name):
       self.rule_name=rule_name
       return self
 
   def set_ds(self, ds):
       self.ds=ds
       return self
 
   def set_dq_table(self, dq_table='test_dq_ssot_audit_log'):
       self.dq_table=dq_table
       return self
 
   def set_table_name(self, table_name):
       self.table_name=table_name
       return self
 
   def set_insert_cols(self, cols):
       self.cols = cols
       self._set_col_exprs()
       return self
 
   def set_filter_expr(self, filter_expr):
       self.filter_expr=filter_expr
       return self
 
   def set_operator_args(self, presto_insert_args):
       self.presto_insert_args=presto_insert_args
       return self
 
   def build_operator(self):
       """
       Builds and returns a PrestoInsertOperatorWithSchema built from this DQBusinessCheckBuilder.
       """
 
       return PrestoInsertOperatorWithSchema(
           partition={
               'ds': self.ds,
               'table_name': self.table_name,
               'rule_name' : self.rule_name
               },
           create=dq_ssot_audit_log_table,
           table=self.dq_table,
           select=self.AUDIT_LOG_SQL.format(
               rule_name=self.rule_name,
               col_names_expr=self.col_names_expr,
               cast_cols_as_varchar_expr=self.cast_cols_as_varchar_expr,
               filter_expr=self.filter_expr,
               table_name=self.table_name,
               ds=self.ds
           ),
           **self.presto_insert_args
       )
 

