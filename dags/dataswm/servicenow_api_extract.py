import datetime
import logging
import time

from dataflow.operators import (
    File2HiveOperator,
    GlobalDefaults,
    ManifoldStore,
    PHPClassMethOperator,
    PythonOperator,
    runcontext,
)
from dataflow_commons.operators import PrestoInsertOperatorWithSchema
from dataflow_commons.operators.schema import Column, HiveAnon, Table
from manifold.clients.python import ManifoldClient
from tasks.consumer_hardware.common.config import Config


GlobalDefaults.set(
    dep_list=[],
    depends_on_past=True,
    oncall=Config.ONCALL,
    schedule="@daily",
    secure_group=Config.DATA_DEV_GROUP,
    user="mikeartz",
)
"""
2021-02-05
Reach out to Mike Artz for questions about this pipeline
"""


retention_days = Config.get_retention_days(runcontext)
logger = logging.getLogger(__name__)

class ServiceNowExporterDataflowAdapterConfig:
    @staticmethod
    def format_select_query(table, columns, ds):
        cols = ",".join([col.name for col in columns])
        return f"SELECT {cols} FROM {table} WHERE ds = '{ds}'"

    @staticmethod
    def generate_table_schema(
        columns, comment, partitions, retention=retention_days, shared_acl=None
    ):
        return Table(
            cols=columns,
            comment=comment,
            partitions=partitions,
            retention=retention,
            reorder_columns=True,
            shared_acl=shared_acl,
        )

    @staticmethod
    def get_mfs(path):
        logger.info(f"creating manifold store for {path}...")
        return ManifoldStore(
            path=path,
            retention_days=7,
            contains_pii=False,
        )

    @staticmethod
    def create_mfs_dir(bucket, path):
        with ManifoldClient.get_client(bucket) as client:
            if not client.sync_exists(path):
                logger.info("directory doesn't exist, creating directory...")
                client.sync_mkdir(path, recursive=True, userData=False)

    @staticmethod
    def rm_mfs_file(bucket, path):
        with ManifoldClient.get_client(bucket) as client:
            logger.info("removing file...")
            if client.sync_exists(path):
                client.sync_rm(path)

    @staticmethod
    def confirm_mfs_files_exist(bucket, path, resource_name, instance):
        def check_files(bucket, path):
            with ManifoldClient.get_client(bucket) as client:
                return [e[0] for e in client.sync_ls(path)]

        now_plus_5 = datetime.datetime.now() + datetime.timedelta(minutes = 5)
        file_list = check_files(bucket, path)
        logger.info("initial file list is ..." + str(file_list))
        logger.info("resource_name is ..." + resource_name)
        while True:
            resource_list = list(
                map(
                    lambda s: s.replace(".tsv", "")
                    .replace(f"zendesk_exporter_dataswarm_adapter_{instance}_", ""),
                    file_list,
                    )
            )
            logger.info("resource_list in loop is ..." + str(resource_list))
            now = datetime.datetime.now()
            if ( resource_name in resource_list):
                break
            elif (now > now_plus_5):
                    raise SystemExit("""
                        TIMEOUT:
                            The files returned from directory {path} do not contain all resource_names.
                            The list of resource_name is {resource_name}.
                            The list of files in the directory {path} is {file_list}.
                            The timeout happens after 5 mintes, which is much longer than needed to
                            get all files normally.  Also there is a limit on number of apikeys that
                            are allowed to an instance of manifoldclient, which will likely cause
                            failure before 5 minutes anyway ( 5 seconds * 50 = 250 seconds and
                             5 minutes > 250 seconds)
                        """.format(resource_name=resource_name, path=path, file_list=file_list)
                    )
            else:
                resource_list = check_files(bucket, path)
                logger.info(resource_list)
                time.sleep(6)
                continue


    servicenow_INSTANCES = ["widget_help_prod"]

    BUCKET = "team_data_dev_group"
    PATH = "servicenow_exporter"

    RESOURCE_TYPES = {
        "brands": {
            "destination_table": "d_servicenow_brands",
            "comment": (
                "Brands are your cust-facing identities. "
                "They might represent multiple products or services, "
                "or they might literally be multiple brands owned and shown by your company."
            ),
            "columns": [
                Column(
                    "id",
                    "BIGINT",
                    "auto assigned when the brand is created. Primary Key",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "brand_name",
                    "VARCHAR",
                    "The name of the brand",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "brand_url", "VARCHAR", "The url of the brand", policy=HiveAnon.NONE
                ),
                Column(
                    "has_help_center",
                    "BOOLEAN",
                    "If the brand has a Help Center",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "help_center_state",
                    "VARCHAR",
                    "The state of the Help Center: enabled, disabled, or restricted",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "is_brand_active",
                    "BOOLEAN",
                    "If the brand is set as active",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "is_default_brand",
                    "BOOLEAN",
                    "Is the brand the default brand for this account",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "brand_created_at",
                    "VARCHAR",
                    "The time the brand was created",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "brand_updated_at",
                    "VARCHAR",
                    "The time of the last update of the brand",
                    policy=HiveAnon.NONE,
                ),
            ],
            "dedup_partition_key": "id",
            "dedup_orderby_key": "brand_updated_at DESC",
            "select": """
            SELECT
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.id') AS BIGINT) AS id,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.name') AS VARCHAR)
                    AS brand_name,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.brand_url') AS VARCHAR
                ) AS brand_url,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.has_help_center') AS BOOLEAN
                ) AS has_help_center,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.help_center_state') AS VARCHAR
                ) AS help_center_state,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.active') AS BOOLEAN)
                    AS is_brand_active,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.default') AS BOOLEAN)
                    AS is_default_brand,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.created_at') AS VARCHAR
                ) AS brand_created_at,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.updated_at') AS VARCHAR
                ) AS brand_updated_at
            FROM {raw_data_table}
            WHERE
                ds = '{ds}'
                AND instance = '{instance}'
                AND resource_name = 'brands'
            """,
        },
        "custom_roles": {
            "destination_table": "d_servicenow_custom_roles",
            "comment": (
                "servicenow help accounts on the organization-wide plan can provide"
                " more granular access to their agents by defining custom agent roles."
            ),
            "columns": [
                Column(
                    "id",
                    "BIGINT",
                    "Primary Key. servicenow auto assigned on creation",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "custom_role_name",
                    "VARCHAR",
                    "Name of the custom role",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "custom_role_description",
                    "VARCHAR",
                    "A description of the role",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "custom_role_created_at",
                    "VARCHAR",
                    "The time the record was created",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "custom_role_updated_at",
                    "VARCHAR",
                    "The time the record was last updated",
                    policy=HiveAnon.NONE,
                ),
            ],
            "dedup_partition_key": "id",
            "dedup_orderby_key": "custom_role_updated_at DESC",
            "select": """
            SELECT
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.id') AS BIGINT) AS id,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.name') AS VARCHAR)
                    AS custom_role_name,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.description') AS VARCHAR
                ) AS custom_role_description,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.created_at') AS VARCHAR
                ) AS custom_role_created_at,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.updated_at') AS VARCHAR
                ) AS custom_role_updated_at
            FROM {raw_data_table}
            WHERE
                ds = '{ds}'
                AND instance = '{instance}'
                AND resource_name = 'custom_roles'
            """,
        },
        "group_memberships": {
            "destination_table": "d_servicenow_group_memberships",
            "comment": (
                "A membership links an agent to a group. "
                "Groups can have many agents, as agents can be in many groups. "
                "You can use the API to list what agents are in which groups, "
                "and reassign group members."
            ),
            "columns": [
                Column(
                    "id",
                    "BIGINT",
                    "Primary Key. servicenow auto assigned upon creation.",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "agent_id", "BIGINT", "The id of an agent", policy=HiveAnon.NONE
                ),
                Column("group_id", "BIGINT", "The id of a group", policy=HiveAnon.NONE),
                Column(
                    "is_default_group",
                    "BOOLEAN",
                    "If true, tickets assigned directly to the agent will "
                    "assume this membership's group.",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "membership_created_at",
                    "VARCHAR",
                    "The time the membership was created",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "membership_updated_at",
                    "VARCHAR",
                    "The time of the last update of the membership",
                    policy=HiveAnon.NONE,
                ),
            ],
            "dedup_partition_key": "id",
            "dedup_orderby_key": "membership_updated_at DESC",
            "select": """
            SELECT
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.id') AS BIGINT) AS id,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.user_id') AS BIGINT)
                    AS agent_id,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.group_id') AS BIGINT)
                    AS group_id,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.default') AS BOOLEAN)
                    AS is_default_group,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.created_at') AS VARCHAR
                ) AS membership_created_at,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.updated_at') AS VARCHAR
                ) AS membership_updated_at
            FROM {raw_data_table}
            WHERE
                ds = '{ds}'
                AND instance = '{instance}'
                AND resource_name = 'group_memberships'
            """,
        },
        "groups": {
            "destination_table": "d_servicenow_groups",
            "comment": (
                "When help requests arrive in servicenow help, they can be assigned to a Group. "
                "Groups serve as the core element of ticket workflow; "
                "help agents are organized into Groups "
                "and tickets can be assigned to a Group only, "
                "or to an assigned agent within a Group."
                "A ticket can never be assigned to an agent without also being assigned to a Group."
            ),
            "columns": [
                Column(
                    "id",
                    "BIGINT",
                    "Primary Key. servicenow auto assigned when creating groups.",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "group_name",
                    "VARCHAR",
                    "The name of the group",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "group_description",
                    "VARCHAR",
                    "The description of the group",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "is_group_default",
                    "BOOLEAN",
                    "If group is default for the account",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "is_group_deleted",
                    "BOOLEAN",
                    "Deleted groups get marked as such",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "group_created_at",
                    "VARCHAR",
                    "The time the group was created",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "group_updated_at",
                    "VARCHAR",
                    "The time of the last update of the group",
                    policy=HiveAnon.NONE,
                ),
            ],
            "dedup_partition_key": "id",
            "dedup_orderby_key": "group_updated_at DESC",
            "select": """
            SELECT
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.id') AS BIGINT) AS id,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.name') AS VARCHAR)
                    AS group_name,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.description') AS VARCHAR
                ) AS group_description,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.default') AS BOOLEAN)
                    AS is_group_default,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.deleted') AS BOOLEAN)
                    AS is_group_deleted,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.created_at') AS VARCHAR
                ) AS group_created_at,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.updated_at') AS VARCHAR
                ) AS group_updated_at
            FROM {raw_data_table}
            WHERE
                ds = '{ds}'
                AND instance = '{instance}'
                AND resource_name = 'groups'
            """,
        },
        "org_memberships": {
            "destination_table": "d_servicenow_org_memberships",
            "comment": (
                "A membership links a user to an org. "
                "orgs can have many users. "
                "Users can be in many orgs if the account helps multiple orgs."
                "You can use the API to list users in an org, "
                "and reassign org members."
            ),
            "columns": [
                Column(
                    "id",
                    "BIGINT",
                    "Primary Key. servicenow auto assigned when the membership is created",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "user_id",
                    "BIGINT",
                    "The ID of the user for whom this memberships belongs",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "org_id",
                    "BIGINT",
                    "The ID of the org associated with this user, in this membership",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "is_org_default",
                    "BOOLEAN",
                    "Denotes whether this is the default org membership for the user. "
                    "If false, returns null",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "org_membership_created_at",
                    "VARCHAR",
                    "When this record was created",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "org_membership_updated_at",
                    "VARCHAR",
                    "When this record last got updated",
                    policy=HiveAnon.NONE,
                ),
            ],
            "dedup_partition_key": "id",
            "dedup_orderby_key": "org_membership_updated_at DESC",
            "select": """
            SELECT
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.id') AS BIGINT) AS id,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.user_id') AS BIGINT)
                    AS user_id,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.org_id') AS BIGINT
                ) AS org_id,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.default') AS BOOLEAN)
                    AS is_org_default,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.created_at') AS VARCHAR
                ) AS org_membership_created_at,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.updated_at') AS VARCHAR
                ) AS org_membership_updated_at
            FROM {raw_data_table}
            WHERE
                ds = '{ds}'
                AND instance = '{instance}'
                AND resource_name = 'org_memberships'
            """,
        },
        "orgs": {
            "destination_table": "d_servicenow_orgs",
            "comment": (
                "Just as agents can be segmented into groups in servicenow help, "
                "your custs (end-users) can be segmented into orgs. "
                "You can manually assign custs to an org or "
                "auto assign them to an org by their email address domain. "
                "orgs can be used in business rules to route tickets "
                "to groups of agents or to send email notifications."
            ),
            "columns": [
                Column(
                    "id",
                    "BIGINT",
                    "Primary Key. servicenow auto assigned when the org is created",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "group_id",
                    "BIGINT",
                    "New tickets from users in this org "
                    "are auto put in this group",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "org_details",
                    "VARCHAR",
                    "Any details obout the org, such as the address",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "org_name",
                    "VARCHAR",
                    "A unique name for the org",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "org_notes",
                    "VARCHAR",
                    "Any notes you have about the org",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "org_custom_fields",
                    "VARCHAR",
                    "Custom fields for this org",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "org_created_at",
                    "VARCHAR",
                    "The time the org was created",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "org_updated_at",
                    "VARCHAR",
                    "The time of the last update of the org",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "org_deleted_at",
                    "VARCHAR",
                    "The time of the deletion of the org",
                    policy=HiveAnon.NONE,
                ),
            ],
            "dedup_partition_key": "id",
            "dedup_orderby_key": "org_updated_at DESC",
            "select": """
            SELECT
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.id') AS BIGINT) AS id,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.group_id') AS BIGINT)
                    AS group_id,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.details') AS VARCHAR)
                    AS org_details,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.name') AS VARCHAR)
                    AS org_name,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.notes') AS VARCHAR)
                    AS org_notes,
                JSON_FORMAT(JSON_EXTRACT(json_value, '$.org_fields'))
                    AS org_custom_fields,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.created_at') AS VARCHAR
                ) AS org_created_at,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.updated_at') AS VARCHAR
                ) AS org_updated_at,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.deleted_at') AS VARCHAR
                ) AS org_deleted_at
            FROM {raw_data_table}
            WHERE
                ds = '{ds}'
                AND instance = '{instance}'
                AND resource_name = 'orgs'
            """,
        },
        "schedules": {
            "destination_table": "d_servicenow_schedules",
            "comment": (
                "You can use the API to create multiple help schedules "
                "with different business hours and holidays. "
                "The API consists of schedule, interval, and holiday objects."
            ),
            "columns": [
                Column(
                    "id",
                    "BIGINT",
                    "Primary Key. servicenow auto assigned upon creation",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "schedule_name",
                    "VARCHAR",
                    "Name of the schedule",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "schedule_time_zone",
                    "VARCHAR",
                    "Time zone of the schedule",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "schedule_created_at",
                    "VARCHAR",
                    "Time the schedule was created",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "schedule_updated_at",
                    "VARCHAR",
                    "Time the schedule was last updated",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "intervals",
                    "VARCHAR",
                    "Intervals are shown with the following attributes:start_time,end_time\n"
                    "An interval represents an active business-hours period.\n"
                    "Interval times are expressed as "
                    "the number of minutes since the start of the week.\n"
                    "Sunday is considered the first day of the week.\n"
                    "For instance, 720 is equivalent to Sunday at noon (12 * 60).",
                    policy=HiveAnon.NONE,
                ),
            ],
            "dedup_partition_key": "id",
            "dedup_orderby_key": "schedule_updated_at DESC",
            "select": """
            SELECT
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.id') AS BIGINT) AS id,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.name') AS VARCHAR)
                    AS schedule_name,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.time_zone') AS VARCHAR
                ) AS schedule_time_zone,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.created_at') AS VARCHAR
                ) AS schedule_created_at,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.updated_at') AS VARCHAR
                ) AS schedule_updated_at,
                JSON_FORMAT(JSON_EXTRACT(json_value, '$.intervals')) AS intervals
            FROM {raw_data_table}
            WHERE
                ds = '{ds}'
                AND instance = '{instance}'
                AND resource_name = 'schedules'
            """,
        },
        "sessions": {
            "destination_table": "d_servicenow_user_sessions",
            "comment": (
                "When an administrator, agent, or end user signs in to a servicenow help instance,"
                " a session is created. "
                "The sessions API lets you view who is currently signed in. "
                "It also lets you terminate one or more sessions. "
                "Terminating a session signs out the user."
            ),
            "columns": [
                Column(
                    "id",
                    "BIGINT",
                    "Primary Key. servicenow auto assigned when the session is created ",
                    policy=HiveAnon.NONE,
                ),
                Column("user_id", "BIGINT", "The id of the user", policy=HiveAnon.NONE),
                Column(
                    "user_session_created",
                    "VARCHAR",
                    "When the session was created",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "session_last_seen_at",
                    "VARCHAR",
                    "The last approximate time this session was seen. "
                    "This does not update on every request.",
                    policy=HiveAnon.NONE,
                ),
            ],
            "dedup_partition_key": "id",
            "dedup_orderby_key": "session_last_seen_at DESC",
            "select": """
            SELECT
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.id') AS BIGINT) AS id,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.user_id') AS BIGINT)
                    AS user_id,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.authenticated_at') AS VARCHAR
                ) AS user_session_created,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.last_seen_at') AS VARCHAR
                ) AS session_last_seen_at
            FROM {raw_data_table}
            WHERE
                ds = '{ds}'
                AND instance = '{instance}'
                AND resource_name = 'sessions'
            """,
        },
        "ticket_events": {
            "destination_table": "f_servicenow_ticket_events",
            "comment": """
            Audits are a read-only history of all updates to a ticket.
            When a ticket is updated in servicenow help, an audit is stored.
            Each audit represents a single update to the ticket.
            An update can consist of one or more events. Examples:

            * The value of a ticket field was changed
            * A new comment was added
            * Tags were added or removed
            * A notification was sent
            """,
            "columns": [
                Column(
                    "id",
                    "BIGINT",
                    "Primary Key. servicenow auto assigned when creating audits",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "ticket_id",
                    "BIGINT",
                    "The ID of the associated ticket",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "created_at",
                    "VARCHAR",
                    "The time the audit was created",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "updater_id",
                    "BIGINT",
                    "The user who created the audit",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "via", "VARCHAR", "how this audit was created", policy=HiveAnon.NONE
                ),
                Column(
                    "event_type",
                    "VARCHAR",
                    "servicenow ticket audit event",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "system",
                    "VARCHAR",
                    "For some channels a source object gives more information about "
                    "how or why the ticket or event was created",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "child_events",
                    "VARCHAR",
                    "An array of the events that happened in this audit.",
                    policy=HiveAnon.NONE,
                ),
            ],
            "dedup_partition_key": "id",
            "dedup_orderby_key": "created_at DESC",
            "select": """
            SELECT
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.id') AS BIGINT) AS id,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.ticket_id') AS BIGINT)
                    AS ticket_id,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.created_at') AS VARCHAR
                ) AS created_at,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.updater_id') AS BIGINT
                ) AS updater_id,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.via') AS VARCHAR) AS via,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.event_type') AS VARCHAR
                ) AS event_type,
                JSON_FORMAT(JSON_EXTRACT(json_value, '$.system')) AS system,
                JSON_FORMAT(JSON_EXTRACT(json_value, '$.child_events')) AS child_events
            FROM {raw_data_table}
            WHERE
                ds = '{ds}'
                AND instance = '{instance}'
                AND resource_name = 'ticket_events'
            """,
        },
        "ticket_metric_events": {
            "destination_table": "f_servicenow_ticket_metric_events",
            "comment": (
                "You can use the ticket metric events API to track reply times, "
                "agent work times, and asker wait times.\n\n"
                "For example, if you want to measure reply times, \n"
                "you can get the time a ticket was created and the"
                " time an agent first replied to it."
                " If you want to measure asker wait times, "
                "you can get the time the ticket was created and "
                "the time its status was changed to solved.\n\n"
                "The times are reported as metric events, "
                "or events that are related to each of the three metrics: "
                "reply time, agent work time, and asker wait time. "
                "You can access the following six types of metric events, "
                "with different events for each type depending on the metric:\n\n"
                "* activate events, such as when a ticket is created"
                " in the case of all three metrics\n"
                "* fulfill events, such as when an agent first replies to a ticket"
                " in the case of the reply time metric, "
                "or when a ticket is solved in the case of the asker wait time metric\n"
                "* pause events, such as when a ticket status is changed "
                "to pending or on-hold in the case of the agent work time metric\n"
                "* apply_sla events, such as when a SLA policy is applied to"
                " a ticket or when an SLA policy or target is changed on a ticket\n"
                "* breach events, such as when a SLA target is breached\n"
                "* update_status events, such as when a metric is fulfilled\n"
            ),
            "columns": [
                Column(
                    "id",
                    "BIGINT",
                    "Primary Key. servicenow auto assgined",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "ticket_id",
                    "BIGINT",
                    "Id of the associated ticket",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "metric_name",
                    "VARCHAR",
                    "One of the following: \n"
                    "    agent_work_time,\n"
                    "    pausable_update_time,\n"
                    "    periodic_update_time, \n"
                    "    reply_time, \n"
                    "    asker_wait_time, \n"
                    "    resolution_time",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "instance_id",
                    "BIGINT",
                    "The instance of the metric associated with the event\n"
                    "Use the instance_id property to track each instance of a metric event\n"
                    "that can occur more than once per ticket, such as the reply_time event.\n"
                    "The value increments over the lifetime of the ticket.",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "metric_event_type",
                    "VARCHAR",
                    "One of the following:\n"
                    "    activate,\n"
                    "    pause, \n"
                    "    fulfill, \n"
                    "    apply_sla,\n"
                    "    breach, \n"
                    "    update_status",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "event_occured_at",
                    "VARCHAR",
                    "The time the event occurred",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "sla",
                    "VARCHAR",
                    "Available if type is apply_sla.\n"
                    "The SLA policy and target being enforced on the ticket"
                    " and metric in question\n"
                    "if any.",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "sla_target",
                    "BIGINT",
                    "flatten SLA object `target` field. ",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "sla_business_hours",
                    "BOOLEAN",
                    "flatten SLA object `business_hours` field",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "sla_policy_id",
                    "BIGINT",
                    "flatten SLA object `policy_id` field. servicenow SLA policy id",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "sla_policy_title",
                    "VARCHAR",
                    "flatten SLA object `policy_title` field. servicenow SLA policy title",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "sla_policy_description",
                    "VARCHAR",
                    "flatten SLA object `policy_description` field. servicenow SLA policy description",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "status",
                    "VARCHAR",
                    "Optional.\n"
                    "The status property provides the number of minutes in both business and\n"
                    "calendar hours for which the metric has been open.\n\n"
                    "The status property is only updated for a fulfill event. \n"
                    "Any ticket metric that hasn't breached yet \n"
                    "or fulfilled at least once won't have a calculated status.",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "status_calendar",
                    "BIGINT",
                    "number of minutes in both calendar hours for which the metric has been open",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "status_business",
                    "BIGINT",
                    "number of minutes in both business hours for which the metric has been open",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "is_event_deleted",
                    "BOOLEAN",
                    "Optional.\n"
                    "The deleted property is only used to indicate whether\n"
                    "or not a breach event should be ignored.\n"
                    "In general, you can ignore any breach event where deleted is true.",
                    policy=HiveAnon.NONE,
                ),
            ],
            "dedup_partition_key": "id",
            "dedup_orderby_key": "event_occured_at DESC",
            "select": """
            SELECT
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.id') AS BIGINT) AS id,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.ticket_id') AS BIGINT)
                    AS ticket_id,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.metric') AS VARCHAR)
                    AS metric_name,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.instance_id') AS BIGINT
                ) AS instance_id,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.type') AS VARCHAR)
                    AS metric_event_type,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.time') AS VARCHAR)
                    AS event_occured_at,
                JSON_FORMAT(JSON_EXTRACT(json_value, '$.sla')) AS sla,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.sla.target') AS BIGINT
                ) AS sla_target,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.sla.business_hours') AS BOOLEAN
                ) AS sla_business_hours,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.sla.policy.id') AS BIGINT
                ) AS sla_policy_id,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.sla.policy.title') AS VARCHAR
                ) AS sla_policy_title,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.sla.policy.description')
                        AS VARCHAR
                ) AS sla_policy_description,
                JSON_FORMAT(JSON_EXTRACT(json_value, '$.status')) AS status,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.status.calendar') AS BIGINT
                ) AS status_calendar,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.status.business') AS BIGINT
                ) AS status_business,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.deleted') AS BOOLEAN)
                    AS is_event_deleted
            FROM {raw_data_table}
            WHERE
                ds = '{ds}'
                AND instance = '{instance}'
                AND resource_name = 'ticket_metric_events'
            """,
        },
        "ticket_metrics": {
            "destination_table": "f_servicenow_ticket_metrics",
            "comment": "Returns a list of tickets with their metrics.",
            "columns": [
                Column(
                    "id",
                    "BIGINT",
                    "Primary Key, servicenow auto assigned",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "ticket_id",
                    "BIGINT",
                    "Id of the associated ticket",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "number_groups_passed",
                    "BIGINT",
                    "Number of groups the ticket passed through",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "number_ticket_assignees",
                    "BIGINT",
                    "Number of assignees the ticket had",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "total_ticket_reopens",
                    "BIGINT",
                    "Total number of times the ticket was reopened",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "total_ticket_replies",
                    "BIGINT",
                    "Total number of times the ticket was replied to",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "assignee_updated_at",
                    "VARCHAR",
                    "When the assignee last updated the ticket",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "asker_updated_at",
                    "VARCHAR",
                    "When the asker last updated the ticket",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "status_updated_at",
                    "VARCHAR",
                    "When the status was last updated",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "initially_assigned_at",
                    "VARCHAR",
                    "When the ticket was initially assigned",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "assigned_at",
                    "VARCHAR",
                    "When the ticket was last assigned",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "solved_at",
                    "VARCHAR",
                    "When the ticket was solved",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "latest_comment_added_at",
                    "VARCHAR",
                    "When the latest comment was added",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "first_resolution_time_calendar_minutes",
                    "BIGINT",
                    "Number of minutes to the first resolution time during calendar hours",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "first_resolution_time_business_minutes",
                    "BIGINT",
                    "Number of minutes to the first resolution time during business hours",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "reply_time_calendar_minutes",
                    "BIGINT",
                    "Number of minutes to the first reply during calendar hours",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "reply_time_business_minutes",
                    "BIGINT",
                    "Number of minutes to the first reply during business hours",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "full_resolution_time_calendar_minutes",
                    "BIGINT",
                    "Number of minutes to the full resolution during calendar hours",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "full_resolution_time_business_minutes",
                    "BIGINT",
                    "Number of minutes to the full resolution during business hours",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "agent_wait_time_calendar_minutes",
                    "BIGINT",
                    "Number of minutes the agent spent waiting during calendar hours",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "agent_wait_time_business_minutes",
                    "BIGINT",
                    "Number of minutes the agent spent waiting during business hours",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "asker_wait_time_calendar_minutes",
                    "BIGINT",
                    "Number of minutes the asker spent waiting during calendar hours",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "asker_wait_time_business_minutes",
                    "BIGINT",
                    "Number of minutes the asker spent waiting during business hours",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "created_at",
                    "VARCHAR",
                    "When the record was created",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "updated_at",
                    "VARCHAR",
                    "When the record was last updated",
                    policy=HiveAnon.NONE,
                ),
            ],
            "dedup_partition_key": "id",
            "dedup_orderby_key": "updated_at DESC",
            "select": """
            SELECT
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.id') AS BIGINT) AS id,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.ticket_id') AS BIGINT)
                    AS ticket_id,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.group_stations') AS BIGINT
                ) AS number_groups_passed,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.assignee_stations') AS BIGINT
                ) AS number_ticket_assignees,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.reopens') AS BIGINT)
                    AS total_ticket_reopens,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.replies') AS BIGINT)
                    AS total_ticket_replies,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.assignee_updated_at') AS VARCHAR
                ) AS assignee_updated_at,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.asker_updated_at') AS VARCHAR
                ) AS asker_updated_at,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.status_updated_at') AS VARCHAR
                ) AS status_updated_at,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.initially_assigned_at')
                        AS VARCHAR
                ) AS initially_assigned_at,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.assigned_at') AS VARCHAR
                ) AS assigned_at,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.solved_at') AS VARCHAR
                ) AS solved_at,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.latest_comment_added_at')
                        AS VARCHAR
                ) AS latest_comment_added_at,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(
                        json_value,
                        '$.first_resolution_time_in_minutes.calendar'
                    ) AS BIGINT
                ) AS first_resolution_time_calendar_minutes,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(
                        json_value,
                        '$.first_resolution_time_in_minutes.business'
                    ) AS BIGINT
                ) AS first_resolution_time_business_minutes,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(
                        json_value,
                        '$.reply_time_in_minutes.calendar'
                    ) AS BIGINT
                ) AS reply_time_calendar_minutes,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(
                        json_value,
                        '$.reply_time_in_minutes.business'
                    ) AS BIGINT
                ) AS reply_time_business_minutes,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(
                        json_value,
                        '$.full_resolution_time_in_minutes.calendar'
                    ) AS BIGINT
                ) AS full_resolution_time_calendar_minutes,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(
                        json_value,
                        '$.full_resolution_time_in_minutes.business'
                    ) AS BIGINT
                ) AS full_resolution_time_business_minutes,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(
                        json_value,
                        '$.agent_wait_time_in_minutes.calendar'
                    ) AS BIGINT
                ) AS agent_wait_time_calendar_minutes,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(
                        json_value,
                        '$.agent_wait_time_in_minutes.business'
                    ) AS BIGINT
                ) AS agent_wait_time_business_minutes,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(
                        json_value,
                        '$.asker_wait_time_in_minutes.calendar'
                    ) AS BIGINT
                ) AS asker_wait_time_calendar_minutes,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(
                        json_value,
                        '$.asker_wait_time_in_minutes.business'
                    ) AS BIGINT
                ) AS asker_wait_time_business_minutes,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.created_at') AS VARCHAR
                ) AS created_at,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.updated_at') AS VARCHAR
                ) AS updated_at
            FROM {raw_data_table}
            WHERE
                ds = '{ds}'
                AND instance = '{instance}'
                AND resource_name = 'ticket_metrics'
            """,
        },
        "ticket_fields": {
            "destination_table": "d_servicenow_ticket_fields",
            "comment": (
                "Returns a list of all system and custom ticket fields in your account."
                "The results are not paginated. Every field is returned in the response."
                "Fields are returned in the order specified by the position and id."
            ),
            "columns": [
                Column(
                    "id",
                    "BIGINT",
                    "auto assigned when created",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "type",
                    "VARCHAR",
                    "System or custom field type.",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "title",
                    "VARCHAR",
                    "The title of the ticket field",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "raw_title",
                    "VARCHAR",
                    "The dynamic content placeholder if present, or the `title` value if not.",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "description",
                    "VARCHAR",
                    "Describes the purpose of the ticket field to users",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "raw_description",
                    "VARCHAR",
                    "The dynamic content placeholder if present, "
                    "or the `description` value if not.",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "position",
                    "BIGINT",
                    "The relative position of the ticket field on a ticket. "
                    "Note that for accounts with ticket forms, "
                    "positions are controlled by the different forms",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "active",
                    "BOOLEAN",
                    "Whether this field is available",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "required",
                    "BOOLEAN",
                    "If true, "
                    "agents must enter a value in the field to change the ticket status to solved",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "collapsed_for_agents",
                    "BOOLEAN",
                    "If true, the field is shown to agents by default. "
                    "If false, the field is hidden alongside infrequently used fields. "
                    "Classic interface only",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "regexp_for_validation",
                    "VARCHAR",
                    "For `regexp` fields only. "
                    "The validation pattern for a field value to be deemed valid",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "title_in_portal",
                    "VARCHAR",
                    "The title of the ticket field for end users in Help Center",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "raw_title_in_portal",
                    "VARCHAR",
                    "The dynamic content placeholder if present, "
                    "or the `title_in_portal` value if not.",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "visibile_in_portal",
                    "BOOLEAN",
                    "Whether this field is visible to end users in Help Center",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "editable_in_portal",
                    "BOOLEAN",
                    "Whether this field is editable by end users in Help Center",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "required_in_portal",
                    "BOOLEAN",
                    "If true, end users must enter a value in the field to create the request",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "tag",
                    "VARCHAR",
                    "For `checkbox` fields only. "
                    "A tag added to tickets when the checkbox field is selected",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "created_at",
                    "VARCHAR",
                    "The time the custom ticket field was created",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "updated_at",
                    "VARCHAR",
                    "	The time the custom ticket field was last updated",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "removable",
                    "BOOLEAN",
                    "If false, this field is a system field that must be present on all tickets",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "agent_description",
                    "VARCHAR",
                    "A description of the ticket field that only agents can see",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "custom_field_options",
                    "VARCHAR",
                    "Required and presented for a custom ticket field "
                    "of type `multiselect` or `tagger`",
                    policy=HiveAnon.NONE,
                ),
            ],
            "dedup_partition_key": "id",
            "dedup_orderby_key": "updated_at DESC",
            "select": """
            SELECT
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.id') AS BIGINT) AS id,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.type') AS VARCHAR) AS type,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.title') AS VARCHAR)
                    AS title,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.raw_title') AS VARCHAR
                ) AS raw_title,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.description') AS VARCHAR
                ) AS description,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.raw_description') AS VARCHAR
                ) AS raw_description,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.position') AS BIGINT)
                    AS "position",
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.active') AS BOOLEAN)
                    AS active,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.required') AS BOOLEAN)
                    AS required,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.collapsed_for_agents') AS BOOLEAN
                ) AS collapsed_for_agents,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.regexp_for_validation')
                        AS VARCHAR
                ) AS regexp_for_validation,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.title_in_portal') AS VARCHAR
                ) AS title_in_portal,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.raw_title_in_portal') AS VARCHAR
                ) AS raw_title_in_portal,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.visible_in_portal') AS BOOLEAN
                ) AS visibile_in_portal,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.editable_in_portal') AS BOOLEAN
                ) AS editable_in_portal,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.required_in_portal') AS BOOLEAN
                ) AS required_in_portal,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.tag') AS VARCHAR) AS tag,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.created_at') AS VARCHAR
                ) AS created_at,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.updated_at') AS VARCHAR
                ) AS updated_at,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.removable') AS BOOLEAN
                ) AS removable,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.agent_description') AS VARCHAR
                ) AS agent_description,
                JSON_FORMAT(JSON_EXTRACT(json_value, '$.custom_field_options'))
                    AS custom_field_options
            FROM {raw_data_table}
            WHERE
                ds = '{ds}'
                AND instance = '{instance}'
                AND resource_name = 'ticket_fields'
            """,
        },
        "tickets": {
            "destination_table": "d_servicenow_tickets",
            "comment": (
                "Tickets are the means through which your end users (custs)"
                " communicate with agents in servicenow help. "
                "Tickets can originate from a number of channels, "
                "including email, Help Center, chat, phone call, Twitter, Facebook,"
                " or the API. All tickets have a core set of properties."
            ),
            "columns": [
                Column(
                    "id",
                    "BIGINT",
                    "Primary Key. servicenow auto assigned when the ticket is created",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "collaborator_user_ids",
                    "ARRAY(BIGINT)",
                    "An array of the numeric IDs of agents or end users to CC.\n"
                    "Note that this replaces any existing collaborators. \n"
                    "An email notification is sent to them when the ticket is created",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "included_user_ids",
                    "ARRAY(BIGINT)",
                    "The ids of agents or end users currently CC'ed on the ticket.\n"
                    "See CCs and followers resources in the help Help Center",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "follower_ids",
                    "ARRAY(BIGINT)",
                    "The ids of agents currently following the ticket. ",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "followup_ticket_ids",
                    "ARRAY(BIGINT)",
                    "The ids of the followups created from this ticket.\n"
                    "Ids are only visible once the ticket is closed",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "assigned_agent_id",
                    "BIGINT",
                    "The agent currently assigned to the ticket",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "brand_id",
                    "BIGINT",
                    "The id of the brand this ticket is associated with",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "forum_topic_id",
                    "BIGINT",
                    "The topic in the servicenow Web portal this ticket originated from, if any.\n"
                    "The Web portal is deprecated",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "group_id",
                    "BIGINT",
                    "The numeric ID of the group to assign the ticket to",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "asker_org_id",
                    "BIGINT",
                    "The numeric ID of the org to assign the ticket to.\n"
                    "The asker must be an end user and a member of the specified org",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "problem_type_id",
                    "BIGINT",
                    "For tickets of type incident, the ID of the problem the incident is linked to",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "requested_by_user_id",
                    "BIGINT",
                    "The user who requested this ticket",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "submitted_by_user_id",
                    "BIGINT",
                    "The user who submitted the ticket. \n"
                    "The submitter always becomes the author of the first comment on the ticket",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "ticket_form_id",
                    "BIGINT",
                    "organization-wide only. The id of the ticket form to render for the ticket",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "record_created_at",
                    "VARCHAR",
                    "When this record was created",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "ticket_description",
                    "VARCHAR",
                    "Read-only first comment on the ticket. \n"
                    "When creating a ticket, use comment to set the description",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "ticket_task_due_at",
                    "VARCHAR",
                    "If this is a ticket of type `task` it has a due date",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "external_id",
                    "BIGINT",
                    "An id you can use to link servicenow help tickets to local records",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "has_incidents",
                    "BOOLEAN",
                    "Is true if a ticket is a problem type and has one \n"
                    "or more incidents linked to it. \n"
                    "Otherwise, the value is false.",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "is_public",
                    "BOOLEAN",
                    "Is true if any comments are public, false otherwise",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "ticket_priority",
                    "VARCHAR",
                    "The urgency with which the ticket should be addressed. \n"
                    "Possible values: `urgent`, `high`, `normal`, `low`",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "ticket_raw_subject",
                    "VARCHAR",
                    "The dynamic content placeholder, if present, or the `subject` value, if not.",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "original_ticket_recipient",
                    "VARCHAR",
                    "The original recipient e-mail address of the ticket",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "ticket_status",
                    "VARCHAR",
                    "The state of the ticket. \n"
                    "Possible values: `new`, `open`, `pending`, `hold`, `solved`, `closed`",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "ticket_subject",
                    "VARCHAR",
                    "The value of the subject field for this ticket",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "ticket_type",
                    "VARCHAR",
                    "The type of this ticket. \n"
                    "Possible values: `problem`, `incident`, `question` or `task`",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "ticket_updated_at",
                    "VARCHAR",
                    "When this record last got updated",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "ticket_via_channel",
                    "VARCHAR",
                    "Channel field in `via` object",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "ticket_via",
                    "VARCHAR",
                    "This object explains how the ticket was created",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "ticket_tags",
                    "ARRAY(VARCHAR)",
                    "The array of tags applied to this ticket",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "custom_fields",
                    "VARCHAR",
                    "Custom fields for the ticket",
                    policy=HiveAnon.NONE,
                ),
            ],
            "dedup_partition_key": "id",
            "dedup_orderby_key": "ticket_updated_at DESC",
            "select": """
            SELECT
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.id') AS BIGINT) AS id,
                TRY_CAST(
                    JSON_EXTRACT(json_value, '$.collaborator_ids') AS ARRAY(BIGINT)
                ) AS collaborator_user_ids,
                TRY_CAST(
                    JSON_EXTRACT(json_value, '$.email_cc_ids') AS ARRAY(BIGINT)
                ) AS included_user_ids,
                TRY_CAST(
                    JSON_EXTRACT(json_value, '$.follower_ids') AS ARRAY(BIGINT)
                ) AS follower_ids,
                TRY_CAST(
                    JSON_EXTRACT(json_value, '$.followup_ids') AS ARRAY(BIGINT)
                ) AS followup_ticket_ids,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.assignee_id') AS BIGINT
                ) AS assigned_agent_id,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.brand_id') AS BIGINT)
                    AS brand_id,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.forum_topic_id') AS BIGINT
                ) AS forum_topic_id,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.group_id') AS BIGINT)
                    AS group_id,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.org_id') AS BIGINT
                ) AS asker_org_id,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.problem_id') AS BIGINT
                ) AS problem_type_id,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.asker_id') AS BIGINT
                ) AS requested_by_user_id,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.submitter_id') AS BIGINT
                ) AS submitted_by_user_id,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.ticket_form_id') AS BIGINT
                ) AS ticket_form_id,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.created_at') AS VARCHAR
                ) AS record_created_at,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.description') AS VARCHAR
                ) AS ticket_description,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.due_at') AS VARCHAR)
                    AS ticket_task_due_at,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.external_id') AS BIGINT
                ) AS external_id,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.has_incidents') AS BOOLEAN
                ) AS has_incidents,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.is_public') AS BOOLEAN
                ) AS is_public,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.priority') AS VARCHAR)
                    AS ticket_priority,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.raw_subject') AS VARCHAR
                ) AS ticket_raw_subject,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.recipient') AS VARCHAR
                ) AS original_ticket_recipient,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.status') AS VARCHAR)
                    AS ticket_status,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.subject') AS VARCHAR)
                    AS ticket_subject,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.type') AS VARCHAR)
                    AS ticket_type,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.updated_at') AS VARCHAR
                ) AS ticket_updated_at,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.via.channel') AS VARCHAR
                ) AS ticket_via_channel,
                JSON_FORMAT(JSON_EXTRACT(json_value, '$.via')) AS ticket_via,
                TRY_CAST(JSON_EXTRACT(json_value, '$.tags') AS ARRAY(VARCHAR))
                    AS ticket_tags,
                JSON_FORMAT(JSON_EXTRACT(json_value, '$.custom_fields'))
                    AS custom_fields
            FROM {raw_data_table}
            WHERE
                ds = '{ds}'
                AND instance = '{instance}'
                AND resource_name = 'tickets'
            """,
        },
        "user_fields": {
            "destination_table": "d_servicenow_user_fields",
            "comment": (
                "servicenow help allows admins to customize fields "
                "displayed on a User profile page. "
                "Basic text fields, date fields, as well as customizable dropdown "
                "and number fields are available. "
                "These fields are currently only visible to agents."
            ),
            "columns": [
                Column(
                    "id",
                    "BIGINT",
                    "Primary Key, servicenow auto assigned upon creation",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "type",
                    "VARCHAR",
                    "Type of the custom field: \n"
                    "`checkbox`, `date`, `decimal`, `dropdown`,"
                    " `integer`, `regexp`, `text`, or `textarea`",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "key",
                    "VARCHAR",
                    "A unique key that identifies this custom field.\n"
                    "This is used for updating the field and referencing in placeholders.",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "title",
                    "VARCHAR",
                    "The title of the custom field",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "raw_title",
                    "VARCHAR",
                    "The dynamic content placeholder, if present, or the `title` value, if not.",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "description",
                    "VARCHAR",
                    "User-defined description of this field's purpose",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "raw_description",
                    "VARCHAR",
                    "The dynamic content placeholder, if present, "
                    "or the `description` value, if not.",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "position",
                    "BIGINT",
                    "Ordering of the field relative to other fields",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "active",
                    "BOOLEAN",
                    "If true, this field is available for use",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "system",
                    "BOOLEAN",
                    "If true, only active and position values of this field can be changed",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "regexp_for_validation",
                    "VARCHAR",
                    "Regular expression field only. \n"
                    "The validation pattern for a field value to be deemed valid.",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "created_at",
                    "VARCHAR",
                    "The time the ticket field was created",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "updated_at",
                    "VARCHAR",
                    "The time of the last update of the ticket field",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "tag",
                    "VARCHAR",
                    "Optional for custom field of type `checkbox`; not presented otherwise.",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "custom_field_options",
                    "VARCHAR",
                    "Required and presented for a custom field of type `dropdown`",
                    policy=HiveAnon.NONE,
                ),
            ],
            "dedup_partition_key": "id",
            "dedup_orderby_key": "updated_at DESC",
            "select": """
            SELECT
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.id') AS BIGINT) AS id,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.type') AS VARCHAR) AS type,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.key') AS VARCHAR) AS key,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.title') AS VARCHAR)
                    AS title,
                JSON_FORMAT(JSON_EXTRACT(json_value, '$.raw_title')) AS raw_title,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.description') AS VARCHAR
                ) AS description,
                TRY_CAST(JSON_EXTRACT(json_value, '$.raw_description') AS VARCHAR)
                    AS raw_description,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.position') AS BIGINT)
                    AS "position",
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.active') AS BOOLEAN)
                    AS active,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.system') AS BOOLEAN)
                    AS system,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.regexp_for_validation')
                        AS VARCHAR
                ) AS regexp_for_validation,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.created_at') AS VARCHAR
                ) AS created_at,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.updated_at') AS VARCHAR
                ) AS updated_at,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.tag') AS VARCHAR) AS tag,
                JSON_FORMAT(JSON_EXTRACT(json_value, '$.custom_field_options'))
                    AS custom_field_options
            FROM {raw_data_table}
            WHERE
                ds = '{ds}'
                AND instance = '{instance}'
                AND resource_name = 'user_fields'
            """,
        },
        "users": {
            "destination_table": "d_servicenow_users",
            "comment": (
                "servicenow help has three types of users: "
                "end-users (your custs), agents, and administrators."
            ),
            "columns": [
                Column(
                    "id",
                    "BIGINT",
                    "Primary Key. auto assigned when the user is created",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "custom_role_id",
                    "BIGINT",
                    "A custom role if the user is an agent on the organization-wide plan",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "default_group_id",
                    "BIGINT",
                    "The id of the user's default group. *Can only be set on create, not on update",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "org_id",
                    "BIGINT",
                    "The id of the user's org.\n"
                    "If the user has more than one org memberships,\n"
                    "the id of the user's default org",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "user_name", "VARCHAR", "The user's name", policy=HiveAnon.USER_NAME
                ),
                Column(
                    "is_user_active",
                    "BOOLEAN",
                    "false if the user has been deleted",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "user_alias",
                    "VARCHAR",
                    "An alias displayed to end users",
                    policy=HiveAnon.UII_REMOVE,
                ),
                Column(
                    "end_user_email",
                    "VARCHAR",
                    "The user's primary email address. *Writable on create only.\n"
                    "On update, a secondary email is added",
                    policy=HiveAnon.EMAIL,
                ),
                Column(
                    "is_chat_only",
                    "BOOLEAN",
                    "Whether or not the user is a chat-only agent",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "user_created_at",
                    "VARCHAR",
                    "The time the user was created",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "user_details",
                    "VARCHAR",
                    "Any details you want to store about the user, such as an address",
                    policy=HiveAnon.UII_REMOVE,
                ),
                Column(
                    "external_system_id",
                    "BIGINT",
                    "A unique identifier from another system.\n"
                    "The API treats the id as case insensitive.",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "user_last_login",
                    "VARCHAR",
                    "The last time the user signed in to servicenow help",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "user_locale",
                    "VARCHAR",
                    "The user's locale. A BCP-47 compliant tag for the locale.\n"
                    "If both `locale` and `locale_id` are present on create or update,\n"
                    "`locale_id` is ignored and only `locale` is used.",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "is_user_moderator",
                    "BOOLEAN",
                    "Designates whether the user has forum moderation capabilities",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "notes_on_user",
                    "VARCHAR",
                    "Any notes you want to store about the user",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "user_primary_phone",
                    "VARCHAR",
                    "The user's primary phone number",
                    policy=HiveAnon.PHONE,
                ),
                Column(
                    "agent_has_restrictions",
                    "BOOLEAN",
                    "If the agent has any restrictions; \n"
                    "false for admins and unrestricted agents, true for other agents",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "user_role",
                    "VARCHAR",
                    "The user's role. Possible values are `end-user`, `agent`, or `admin`",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "role_type",
                    "BIGINT",
                    "The user's role id. \n"
                    "0 for custom agents, \n"
                    "1 for light agent, \n"
                    "2 for chat agent, \n"
                    "and 3 for chat agent added to the help account as a contributor",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "is_user_shared",
                    "BOOLEAN",
                    "If the user is shared from a different servicenow help instance. \n"
                    "Ticket sharing accounts only",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "is_agent_shared",
                    "BOOLEAN",
                    "If the user is a shared agent from a different servicenow help instance. \n"
                    "Ticket sharing accounts only",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "is_agent_suspended",
                    "BOOLEAN",
                    "If the agent is suspended. "
                    "Tickets from suspended users are also suspended, "
                    "and these users cannot sign in to the end user portal",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "user_timezone",
                    "VARCHAR",
                    "The user's time zone",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "user_last_updated_at",
                    "VARCHAR",
                    "The time the user was last updated",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "user_verified",
                    "BOOLEAN",
                    "The user's primary identity is verified or not.",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "user_tags",
                    "ARRAY(VARCHAR)",
                    "The user's tags. ",
                    policy=HiveAnon.NONE,
                ),
                Column(
                    "user_fields",
                    "VARCHAR",
                    "Values of custom fields in the user's profile.",
                    policy=HiveAnon.NONE,
                ),
            ],
            "dedup_partition_key": "id",
            "dedup_orderby_key": "user_last_updated_at DESC",
            "select": """
            SELECT
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.id') AS BIGINT) AS id,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.custom_role_id') AS BIGINT
                ) AS custom_role_id,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.default_group_id') AS BIGINT
                ) AS default_group_id,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.org_id') AS BIGINT
                ) AS org_id,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.name') AS VARCHAR)
                    AS user_name,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.active') AS BOOLEAN)
                    AS is_user_active,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.alias') AS VARCHAR)
                    AS user_alias,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.email') AS VARCHAR)
                    AS end_user_email,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.chat_only') AS BOOLEAN
                ) AS is_chat_only,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.created_at') AS VARCHAR
                ) AS user_created_at,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.details') AS VARCHAR)
                    AS user_details,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.external_id') AS BIGINT
                ) AS external_system_id,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.last_login_at') AS VARCHAR
                ) AS user_last_login,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.local') AS VARCHAR)
                    AS user_locale,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.moderator') AS BOOLEAN
                ) AS is_user_moderator,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.notes') AS VARCHAR)
                    AS notes_on_user,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.phone') AS VARCHAR)
                    AS user_primary_phone,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.restricted_agent') AS BOOLEAN
                ) AS agent_has_restrictions,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.role') AS VARCHAR)
                    AS user_role,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.role_type') AS BIGINT)
                    AS role_type,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.shared') AS BOOLEAN)
                    AS is_user_shared,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.shared_agent') AS BOOLEAN
                ) AS is_agent_shared,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.suspended') AS BOOLEAN
                ) AS is_agent_suspended,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.time_zone') AS VARCHAR
                ) AS user_timezone,
                TRY_CAST(
                    JSON_EXTRACT_SCALAR(json_value, '$.updated_at') AS VARCHAR
                ) AS user_last_updated_at,
                TRY_CAST(JSON_EXTRACT_SCALAR(json_value, '$.verified') AS BOOLEAN)
                    AS user_verified,
                TRY_CAST(JSON_EXTRACT(json_value, '$.tags') AS ARRAY(VARCHAR))
                    AS user_tags,
                JSON_FORMAT(JSON_EXTRACT(json_value, '$.user_fields')) AS user_fields
            FROM {raw_data_table}
            WHERE
                ds = '{ds}'
                AND instance = '{instance}'
                AND resource_name = 'users'
            """,
        },
    }


bucket = ServiceNowExporterDataflowAdapterConfig.BUCKET
path = ServiceNowExporterDataflowAdapterConfig.PATH
handle_mfs_dir_func = {}
validate_file_drop_from_servicenow_exporter_dataflow_adapter = {}
cleanup_mfs_file = {}
load_f_servicenow_exporter_raw = {}
load_f_servicenow_exporter_staging_tables = {}
load_f_servicenow_exporter_tables = {}
fetch_from_servicenow_exporter_dataflow_adapter = {}
for instance in ServiceNowExporterDataflowAdapterConfig.servicenow_INSTANCES:
    raw_data_hive_table = "<TABLE:f_servicenow_exporter_raw>"
    resource_list = list(ServiceNowExporterDataflowAdapterConfig.RESOURCE_TYPES.keys())
    logger.info("resource_list is ... {rl}".format(rl=str(resource_list)))

    handle_mfs_dir_func[instance] = PythonOperator(
        dep_list=[],
        func=ServiceNowExporterDataflowAdapterConfig.create_mfs_dir,
        func_args=[bucket, f"graph/{path}"]
    )

    load_f_servicenow_exporter_staging_tables[instance] = {}
    load_f_servicenow_exporter_tables[instance] = {}
    cleanup_mfs_file[instance] = {}
    fetch_from_servicenow_exporter_dataflow_adapter[instance] = {}
    load_f_servicenow_exporter_raw[instance] = {}
    partitions = [
        Column("ds", "VARCHAR", "ds partition", policy=HiveAnon.NONE),
        Column(
            "instance",
            "VARCHAR",
            "instance partition. `widget_help_prod` is available",
            policy=HiveAnon.NONE,
        ),
    ]
    for (
        resource_name,
        resource_config,
    ) in ServiceNowExporterDataflowAdapterConfig.RESOURCE_TYPES.items():

        mls_file = f"graph/{path}/servicenow_exporter_dataflow_adapter_{instance}_{resource_name}.tsv"
        cleanup_mfs_file[instance][resource_name] = PythonOperator(
            dep_list=[handle_mfs_dir_func[instance]],
            func=ServiceNowExporterDataflowAdapterConfig.rm_mfs_file,
            func_args=[bucket, mls_file]
        )

        input_file = f"{path}/servicenow_exporter_dataflow_adapter_{instance}.tsv"
        fetch_from_servicenow_exporter_dataflow_adapter[instance][resource_name] = PHPClassMethOperator(
            dep_list=[cleanup_mfs_file[instance][resource_name]],
            class_name="servicenowExporterdataflowAdapter",
            meth_name="gen",
            max_retry=2,
            func_args=[
                input_file,
                "<DATETIME:%Y-%m-%d %H-%M-%S|days=-1>",
                instance,
                [resource_name],
            ],
        )

        validate_file_drop_from_zendesk_exporter_dataswarm_adapter[instance][resource_name] = PythonOperator(
            dep_list=[cleanup_mfs_file[instance][resource_name]],
            func=ServiceNowExporterDataflowAdapterConfig.confirm_mfs_files_exist,
            func_args=[bucket, f"graph/{path}", resource_name, instance]
        )

        mfs_file = f"{bucket}/graph/{path}/servicenow_exporter_dataflow_adapter_{instance}_{resource_name}.tsv"
        load_f_servicenow_exporter_raw[instance][resource_name] = File2HiveOperator(
            dep_list=[fetch_from_servicenow_exporter_dataflow_adapter[instance][resource_name]],
            filepath='ignore',
            filestore=ServiceNowExporterDataflowAdapterConfig.get_mfs(mfs_file),
            hive_partition=f"ds='<DATEID>',instance='{instance}'",
            hive_table=raw_data_hive_table,
            hive_setup=f"""
            CREATE TABLE IF NOT EXISTS {raw_data_hive_table} (
                resource_name STRING,
                json_value STRING
            )
            PARTITIONED BY (ds STRING, instance STRING)
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY '\t'
            TBLPROPERTIES('RETENTION' = '{retention_days}', 'noUII' = '0')
            """,
        )

        columns = resource_config["columns"]
        comment = resource_config["comment"]
        select = resource_config["select"].format(
            raw_data_table=raw_data_hive_table, ds="<DATEID>", instance=instance
        )
        staging_table = "<TABLE:{table_name}>".format(
            table_name=resource_config["destination_table"] + "_staging"
        )

        create = ServiceNowExporterDataflowAdapterConfig.generate_table_schema(
            columns=columns,
            comment=comment,
            partitions=partitions,
            retention=retention_days,
            shared_acl=Config.SHARED_ACL_STAGING,
        )

        load_f_servicenow_exporter_staging_tables[instance][
            resource_name
        ] = PrestoInsertOperatorWithSchema(
            dep_list=[load_f_servicenow_exporter_raw[instance]],
            partition={"ds": "<DATEID>", "instance": instance},
            create=create,
            select=select,
            table=staging_table,
        )

        destination_table = "<TABLE:{table_name}>".format(
            table_name=resource_config["destination_table"]
        )
        dedup_partition_key = resource_config["dedup_partition_key"]
        dedup_orderby_key = resource_config["dedup_orderby_key"]
        select = """
        WITH unioned_prev_and_curr AS (
            {prev}
            UNION ALL
            {curr}
        ),
        ordered_prev_and_curr AS (
            SELECT
                a.*,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        {dedup_partition_key}
                    ORDER BY
                        {dedup_orderby_key}
                ) AS row_num
            FROM unioned_prev_and_curr AS a
        )
        SELECT
            {selected_columns}
        FROM ordered_prev_and_curr
        WHERE
            row_num = 1
        """.format(
            prev=ServiceNowExporterDataflowAdapterConfig.format_select_query(
                destination_table, columns, "<DATEID-1>"
            ),
            curr=ServiceNowExporterDataflowAdapterConfig.format_select_query(
                staging_table, columns, "<DATEID>"
            ),
            dedup_partition_key=dedup_partition_key,
            dedup_orderby_key=dedup_orderby_key,
            selected_columns=",".join([col.name for col in columns]),
        )
        load_f_servicenow_exporter_tables[instance][
            resource_name
        ] = PrestoInsertOperatorWithSchema(
            dep_list=[load_f_servicenow_exporter_staging_tables[instance][resource_name]],
            table=destination_table,
            partition={"ds": "<DATEID>", "instance": instance},
            create=create,
            select=select,
        )