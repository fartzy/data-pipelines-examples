#!/usr/bin/env python3
 
from dataflow.operators import (
   GlobalDefaults,
   WaitForHiveOperator,
   MixedSourceDqOperator,
   runcontext,
)
 
from dataflow_commons.operators import PrestoInsertOperatorWithSchema
from dataflow_commons.operators.schema import Column, HiveAnon, Table
from tasks.consumer_hardware.common.config import Config
from tasks.consumer_hardware.ops_info.dq.common.utils import (
   dq_ssot_audit_log_table,
   dq_ssot_audit_aggregate_log_table,
   retention_days,
   Util,
   DQBusinessCheckBuilder
)
 
GlobalDefaults.set(
   dep_list=[],
   depends_on_past=True,
   dq_do_not_stop_pipeline=True,
   extra_emails=[
       "michaelartz@acme.com"
   ],
   fail_on_empty=False,
   oncall="operations_ops_info_team",
   secure_group="acmeoperations_ops_info",
   task_oncall="operations_ops_info_team",
   schedule="@daily",
   task_pri="mid",
   user="michaelartz",
)
 
table_name = '<TABLE:target_table>'
dq_table = '<TABLE:dq_ssot_audit_log>'
dq_agg_table = '<TABLE:dq_ssot_audit_aggregate_log>'
dq_ds = '<DATEID>'
dq_ds_minus_1 = '<DATEID-1>'
 
# these 'core_cols' are used to identify a row.  They can be used later on for analysis
# of a particular business check not being met. The row can be looked up by these columns
# and inspected later on.
core_insert_cols = [
   'order_number',
   'fulfill_line_id',
   'order_ship_item_id',
   'data_source',
]
 
wait_for_target_table = WaitForHiveOperator(table=table_name)
 
# Ensure none of the below business dates should be before the time that the order is created
# If the business date is NULL, then it is fine though
creation_time_gt_date_cols = [
   'request_date',
   'est_deliver_date',
   'est_ship_date',
   'ship_date',
   'initial_ship_date',
   'initial_deliver_date',
   'deliver_date'
]
 
order_creation_time_gt_date_col = {}
for insert_cols in [core_insert_cols + [date_col] for date_col in creation_time_gt_date_cols]:
 
   date_col = insert_cols[-1]
   dq_args = {
       "dep_list": [wait_for_target_table],
       "documentation": {
           "description" : f"Ensure {date_col} isn't before the time that the order is created."}
       }
 
   order_creation_time_gt_date_col[date_col] = (
       DQBusinessCheckBuilder()
           .set_ds(dq_ds)
           .set_dq_table(dq_table)
           .set_table_name(table_name)
           .set_rule_name(f"order_creation_time_gt_{date_col}")
           .set_insert_cols(insert_cols)
 
           # Converting to date to get the 00:00:00.000 for that day to try to avoid
           # timezone differences being the reason this is data quality check is met
           .set_filter_expr(f"COALESCE({date_col}, CURRENT_TIMESTAMP) < DATE(order_creation_time)")
           .set_operator_args(dq_args)
           .build_operator()
   )
 
# Ensure there are no ordered_quantity less than num_units_shipped
# num_units_shipped_gt_ordered_quantity
dq_args = {
   "dep_list": [wait_for_target_table],
   "documentation": {"description":"Ensure there are no ordered_quantity less than num_units_shipped."}
   }
 
num_units_shipped_gt_ordered_quantity = (
   DQBusinessCheckBuilder()
       .set_ds(dq_ds)
       .set_dq_table(dq_table)
       .set_table_name(table_name)
       .set_rule_name('test_num_units_shipped_gt_ordered_quantity')
       .set_insert_cols(core_insert_cols + ['ordered_quantity','num_units_shipped'])
       .set_filter_expr('ordered_quantity < num_units_shipped')
       .set_operator_args(dq_args)
       .build_operator()
)
 
# Ensure there are no est_ship_date greater than est_deliver_date
# est_ship_date_gt_est_deliver_date
dq_args = {
   "dep_list": [wait_for_target_table],
   "documentation": {
       "description" : """
           - Ensure there are no est_ship_date greater than est_deliver_date
       """}
   }
 
est_ship_date_gt_est_deliver_date = (
   DQBusinessCheckBuilder()
       .set_ds(dq_ds)
       .set_dq_table(dq_table)
       .set_table_name(table_name)
       .set_rule_name('est_ship_date_gt_est_deliver_date')
       .set_insert_cols(core_insert_cols + ['est_ship_date','est_deliver_date'])
       .set_filter_expr('est_ship_date > est_deliver_date')
       .set_operator_args(dq_args)
       .build_operator()
)
 
# Ensure there are no ship_date greater than deliver_date
# ship_date_gt_deliver_date
dq_args = {
   "dep_list": [wait_for_target_table],
   "documentation": {
       "description" : """
           - Ensure there are no ship_date greater than deliver_date
       """}
   }
 
ship_date_gt_deliver_date = (
   DQBusinessCheckBuilder()
       .set_ds(dq_ds)
       .set_dq_table(dq_table)
       .set_table_name(table_name)
       .set_rule_name('ship_date_gt_deliver_date')
       .set_insert_cols(core_insert_cols + ['deliver_date','ship_date'])
       .set_filter_expr('deliver_date < ship_date')
       .set_operator_args(dq_args)
       .build_operator()
)
 
 
# Ensure that if an order_number accompanying line_flow_status should be in sync when, specifically
# for being 'CLOSED'.  In other words, since every record has a line_flow_status, then if every record
# that has the same order_status has the value 'CLOSED' for line_flow_staus, then all of those rows
# should also have 'CLOSED' for their order_status.
# order_status_not_closed
dq_args = {
   "dep_list": [wait_for_target_table],
   "documentation": {
       "description" : """
           - Ensure that if an order_number accompanying line_flow_status should be in sync when, specifically
             for being 'CLOSED'.  In other words, since every record has a line_flow_status, if every record
             that has the same order_status has the value 'CLOSED' for line_flow_staus, then those rows
             should also have 'CLOSED' for their order_status.
       """}
   }
 
order_status_not_closed = (
   DQBusinessCheckBuilder()
       .set_ds(dq_ds)
       .set_dq_table(dq_table)
       .set_table_name(table_name)
       .set_rule_name('order_status_not_closed')
       .set_insert_cols(['order_number'])
       .set_filter_expr("""
           EXISTS (
                   SELECT
                       1
                   FROM <TABLE:target_table> ffa2
                   WHERE
                       base_table.order_number = ffa2.order_number
                       AND ffa2.order_status <> 'CLOSED'
                  )
           GROUP BY
               order_number
           HAVING
               APPROX_DISTINCT(line_flow_status) = 1
               AND MAX(line_flow_status) = 'CLOSED'
           """)
       .set_operator_args(dq_args)
       .build_operator()
)
 
 
# Ensure there are no first_hold_date greater than latest_released_hold_date
# first_hold_date_gt_latest_released_hold_date
dq_args = {
   "dep_list": [wait_for_target_table],
   "documentation": {
       "description" : """
           - Ensure there are no first_hold_date greater than latest_released_hold_date
       """}
   }
 
first_hold_date_gt_latest_released_hold_date = (
   DQBusinessCheckBuilder()
       .set_ds(dq_ds)
       .set_dq_table(dq_table)
       .set_table_name(table_name)
       .set_rule_name('first_hold_date_gt_latest_released_hold_date')
       .set_insert_cols(core_insert_cols + ['first_hold_date','latest_released_hold_date'])
       .set_filter_expr('latest_released_hold_date < first_hold_date')
       .set_operator_args(dq_args)
       .build_operator()
)
 
# Ensure there are no rma_reference that does not exist in the table as an "order_number"
# invalid_rma_reference
dq_args = {
   "dep_list": [wait_for_target_table],
   "documentation": {
       "description" : """
           - Ensure there are no rma_reference that does not exist in the table as an 'order_number'
       """}
   }
 
invalid_rma_reference = (
   DQBusinessCheckBuilder()
       .set_ds(dq_ds)
       .set_dq_table(dq_table)
       .set_table_name(table_name)
       .set_rule_name('invalid_rma_reference')
       .set_insert_cols(core_insert_cols + ['rma_reference'])
       .set_filter_expr("""
               NOT EXISTS (
                   SELECT
                       1
                   FROM <TABLE:target_table> ffa2
                   WHERE
                       base_table.rma_reference = ffa2.order_number
               )
       """)
       .set_operator_args(dq_args)
       .build_operator()
)
 
# Ensure there are no records where the fulfilled_quantity does not equal
# the original_num_units_shipped or num_units_shipped
# fulfilled_quantity_ne_num_units_shipped
dq_args = {
   "dep_list": [wait_for_target_table],
   "documentation": {
       "description" : """
           - Ensure there are no records where the fulfilled_quantity does not equal
             the original_num_units_shipped or num_units_shipped
       """}
   }
 
fulfilled_quantity_ne_num_units_shipped = (
   DQBusinessCheckBuilder()
       .set_ds(dq_ds)
       .set_dq_table(dq_table)
       .set_table_name(table_name)
       .set_rule_name('fulfilled_quantity_ne_num_units_shipped')
       .set_insert_cols(core_insert_cols + ['num_units_shipped','fulfilled_quantity','original_num_units_shipped'])
       .set_filter_expr("""
           fulfilled_quantity IS NOT NULL
           AND fulfilled_quantity <> num_units_shipped
       """)
       .set_operator_args(dq_args)
       .build_operator()
)
 
# Ensure if order_status = 'CANCELED', then all the lines should have 'CANCELED' status
# and num_units_shipped is null or 0 for all the lines.
# line_status_cancelled_and_num_units_shipped_is_null_or_zero
dq_args = {
   "dep_list": [wait_for_target_table],
   "documentation": {
       "description" : """
           - Ensure if order_status = 'CANCELED', then all the lines should have 'CANCELED' status
             and num_units_shipped is null or 0 for all the lines.
       """}
   }
 
line_status_cancelled_and_num_units_shipped_is_null_or_zero = (
   DQBusinessCheckBuilder()
       .set_ds(dq_ds)
       .set_dq_table(dq_table)
       .set_table_name(table_name)
       .set_rule_name('line_status_cancelled_and_num_units_shipped_is_null_or_zero')
       .set_insert_cols(core_insert_cols + ['order_status','line_flow_status','num_units_shipped'])
       .set_filter_expr("""
                   order_status = 'CANCELLED'
                   AND (
                       line_flow_status <> 'CANCELLED'
                       OR COALESCE(num_units_shipped, 0) <> 0
                   )
       """)
       .set_operator_args(dq_args)
       .build_operator()
)
 
# If order_fulfillment_status = 'CANCELED' then num_units_shipped is null or 0 for the fulfillment line.
# ordful_status_cancelled_and_num_units_null_or_zero
dq_args = {
   "dep_list": [wait_for_target_table],
   "documentation": {
       "description" : """
           - If order_fulfillment_status = 'CANCELED' then num_units_shipped is null or 0 for the
             fulfillment line.
       """}
   }
 
ordful_status_cancelled_and_num_units_null_or_zero = (
   DQBusinessCheckBuilder()
       .set_ds(dq_ds)
       .set_dq_table(dq_table)
       .set_table_name(table_name)
       .set_rule_name('ordful_status_cancelled_and_num_units_null_or_zero')
       .set_insert_cols(core_insert_cols + ['order_fulfillment_status','line_flow_status','num_units_shipped'])
       .set_filter_expr("""
                   order_fulfillment_status = 'CANCELLED'
                   AND (
                       COALESCE(num_units_shipped, 0) <> 0
                   )
       """)
       .set_operator_args(dq_args)
       .build_operator()
)
 
 
# The columns below have a low cardinality and if new values are added in these columns then someone might
# be interested.  So there is an email notification sent out.  Add more emails to the email notification
# parameter if needed
low_cardinality_cols = [
   'order_type',
   'transactional_currency_code',
   'order_transaction_type',
   'subinventory',
   'order_status',
   'line_flow_status',
   'order_ship_status',
   'ship_service',
   'deliver_lead_time'
]
 
# Insert into dq_ssot_audit_aggregate_log_table the aggregates for the next day comparison
dq_ssot_audit_aggregate_log_insert = PrestoInsertOperatorWithSchema(
   dep_list=[wait_for_target_table],
   documentation={
       "description": """Summary:
       -   This table will store consolidated data quality check aggregate information
           for the order fulfillment, rma, inventory snapshot, material distribution,
           channel sales, invoices, b2b, manufacturer_po_8##, partner_support, customer support,
           and dim_ce_items tables.
 
       SLA Info:
       - Data expected to land by 8AM PT daily
       """
   },
   table=dq_agg_table,
   partition={
       'ds': dq_ds,
       'table_name': table_name
       },
   create=dq_ssot_audit_aggregate_log_table,
   select=Util.AUDIT_AGG_LOG_INSERT_SQL.format(
       **Util.get_agg_target_select_args(low_cardinality_cols, table_name, dq_ds)
   )
)
 
# Compare the selected columns over the last day and flag when a value has changed.
# These are for very low cardinality columns where change is not expected often.
dq_ssot_audit_aggregate_log_compare = {}
for col_name in low_cardinality_cols:
   dq_ssot_audit_aggregate_log_compare[col_name] = MixedSourceDqOperator(
       documentation = {
           "description" : """
               Compare the selected columns over the last day and flag when a value has changed
               These are for very low cardinality columns where change is not expected often
           """
       },
       dep_list=[dq_ssot_audit_aggregate_log_insert],
       dq_do_not_stop_pipeline=True,
       check_name=f"new_{col_name}_value_introduced_into_warehouse",
       source_datasource="presto",
       source_query=Util.SOURCE_QUERY_AGG_LOG_SQL.format(
           col_name=col_name,table_name=table_name, ds=dq_ds
           ),
       target_datasource="presto",
       target_query=Util.TARGET_QUERY_AGG_LOG_SQL.format(
           col_name=col_name,dq_agg_table=dq_agg_table, ds_minus_1=dq_ds_minus_1
           ),
       equals=True,
       notify=["task", "email"],
       nofify_cc=["michaelartz@acme.com"],
       alert_message="""
           [NEW_VALUE] A new {col_name} has been introduced into the target_table table
           The below query can be run to see which value(s) has been introduced
 
           Daiquery Link - https://www.internalacme.com/intern/querytool/workspace/
 
           {user_query}
       """.format(col_name=col_name, user_query=Util.USER_QUERY.format(
               col_name=col_name, table_name=table_name, ds=dq_ds, ds_minus_1=dq_ds_minus_1)
           )
   )

