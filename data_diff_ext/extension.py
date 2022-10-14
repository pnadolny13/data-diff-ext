"""Meltano DataDiff extension."""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import structlog
from data_diff import connect_to_table, diff_tables
from meltano.edk import models
from meltano.edk.extension import ExtensionBase
from meltano.edk.process import Invoker, log_subprocess_error

log = structlog.get_logger()


class DataDiff(ExtensionBase):
    """Extension implementing the ExtensionBase interface."""

    def __init__(self) -> None:
        """Initialize the extension."""
        self.data_diff_bin = "data-diff"  # verify this is the correct name
        self.data_diff_invoker = Invoker(self.data_diff_bin)

    def invoke(self, command_name: str | None, *command_args: Any) -> None:
        """Invoke the underlying cli, that is being wrapped by this extension.

        Args:
            command_name: The name of the command to invoke.
            command_args: The arguments to pass to the command.
        """
        try:
            self.data_diff_invoker.run_and_log(command_name, *command_args)
        except subprocess.CalledProcessError as err:
            log_subprocess_error(
                f"data_diff {command_name}", err, "DataDiff invocation failed"
            )
            sys.exit(err.returncode)

    def describe(self) -> models.Describe:
        """Describe the extension.

        Returns:
            The extension description
        """
        # TODO: could we auto-generate all or portions of this from typer instead?
        return models.Describe(
            commands=[
                models.ExtensionCommand(
                    name="data_diff_extension", description="extension commands"
                ),
                models.InvokerCommand(
                    name="data_diff_invoker", description="pass through invoker"
                ),
            ]
        )

    def dbt_diff_tables(self):
        """Describe the extension.

        Returns:
            The extension description
        """
        account = ''
        password = ''
        # connect_dev = {
        #     'driver': 'snowflake',
        #     'user': 'PNADOLNY',
        #     'password': f'{password}',
        #     'account': f'{account}',
        #     'database': 'USERDEV_PROD',
        #     'schema': 'PNADOLNY_TELEMETRY',
        #     'warehouse': 'CORE',
        #     'role': 'PNADOLNY'
        # }
        # connect_prod = {
        #     'driver': 'snowflake',
        #     'user': 'PNADOLNY',
        #     'password': f'{password}',
        #     'account': f'{account}',
        #     'database': 'PROD',
        #     'schema': 'TELEMETRY',
        #     'warehouse': 'CORE',
        #     'role': 'PNADOLNY'
        # }

        # table1 = connect_to_table(connect_dev, "FACT_CLI_EXECUTIONS", "EXECUTION_ID")
        # table2 = connect_to_table(connect_prod, "FACT_CLI_EXECUTIONS", "EXECUTION_ID")

        # for different_row in diff_tables(
        #     table1,
        #     table2,
        #     bisection_threshold=100000,
        #     bisection_factor=6,
        #     debug=True
        # ):
        #     plus_or_minus, columns = different_row
        #     print(plus_or_minus, columns)
        PROJECT_ROOT = os.getenv("MELTANO_PROJECT_ROOT", os.getcwd())
        MELTANO_BIN = ".meltano/run/bin"

        if not Path(PROJECT_ROOT).joinpath(MELTANO_BIN).exists():
            MELTANO_BIN = "meltano"

        tables = [
            ('PROD', 'TELEMETRY', 'PIPELINE_DIM', ['pipeline_pk', 'project_id', 'env_id', 'plugin_count']),
            ('PROD', 'TELEMETRY', 'DAILY_ACTIVE_PROJECTS', ['project_id']),
            ('PROD', 'TELEMETRY', 'FACT_CLI_PROJECTS', ['project_id']),
            ('PROD', 'TELEMETRY', 'FACT_CLI_EXECUTIONS', ['execution_id', 'project_id', 'pipeline_fk', 'event_count' ,'cli_command', 'exit_code', 'is_active_eom_cli_execution','monthly_piplines_active_eom' ,'is_exec_event', 'is_ci_environment', 'cli_runtime_ms']),
            ('PROD', 'TELEMETRY', 'FACT_PLUGIN_USAGE', ['plugin_exec_pk']),
            ('PROD', 'TELEMETRY', 'PROJECT_DIM', ['project_id']),
            ('PREP', 'WORKSPACE', 'CLI_EXECS_BLENDED', ['execution_id']),
            ('PREP', 'WORKSPACE', 'STRUCTURED_EXECUTIONS', ['execution_id']),
            ('PREP', 'WORKSPACE', 'CLI_EXECUTIONS_BASE', ['execution_id']),
            ('PREP', 'WORKSPACE', 'UNSTRUCTURED_EXECUTIONS', ['execution_id']),
            ('PREP', 'WORKSPACE', 'UNSTRUCT_EVENT_FLATTENED', ['event_id']),
            ('PREP', 'WORKSPACE', 'OPT_OUTS', ['project_id', 'opted_out_at']),
            ('PREP', 'WORKSPACE', 'UNSTRUCT_EXEC_FLATTENED', ['execution_id']),
            ('PREP', 'WORKSPACE', 'STRUCT_PLUGIN_EXECUTIONS', ['struct_plugin_exec_pk']),

            # Extras or not working yet
            # ('PREP', 'WORKSPACE', 'CONTEXT_BASE', ['event_id', 'context', 'context_index']),
            # ('PREP', 'WORKSPACE', 'CMD_PARSED_ALL', ['command']),
            # ('PREP', 'WORKSPACE', 'ACTIVE_PROJECTS_BASE', ['date_day']),
            # ('TELEMETRY', 'TEMP_DAILY_ACTIVE_PROJECTS_1D', ''),
            # ('WORKSPACE', 'TEMP_ACTIVE_PROJECTS_BASE_1D', ''),
            # ('TELEMETRY', 'FACT_CLI_COHORTS', ''),
            # ('TELEMETRY', 'PROJECT_FUNNEL_COHORT', ''),
            # ('TELEMETRY', 'TEMP_FACT_CLI_EXECUTIONS_1D', ''),
            # ('TELEMETRY', 'ACTIVE_PROJECTS_28D', ''),
            # ('TELEMETRY', 'PROJECT_PLUGINS', ''),

        ]
        user_prefix = 'PNADOLNY'
        for (db, schema, table, col_list) in tables:
            key_col = ''
            extra_cols = ''
            for index, col in enumerate(col_list):
                if index == 0:
                    key_col = col
                extra_cols += f'-c {col} '
            command = f"{MELTANO_BIN} invoke data-diff --stats --key-column {key_col} {extra_cols}--bisection-threshold 100000 --verbose snowflake://PNADOLNY:{password}@{account}/USERDEV_{db}/{user_prefix}_{schema}?warehouse=CORE&role=PNADOLNY {table} snowflake://PNADOLNY:{password}@{account}/{db}/{schema}?warehouse=CORE&role=PNADOLNY {table}"
            log.info(f"Diffing: {user_prefix}_{schema}.{table} against {schema}.{table}")
            try:
                list_result = subprocess.run(
                    command.split(" "),
                    cwd=PROJECT_ROOT,
                    stdout=subprocess.PIPE,
                    universal_newlines=True,
                    check=True,
                )
                log.info(list_result.stdout)
            except Exception as e:
                return e
