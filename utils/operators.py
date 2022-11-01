"""
Helper module for connecting to a MSSQL database and executing SQL queries remotely
through airflow
"""
from __future__ import annotations

import logging

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from utils.hooks import MSSQLHook


class MSSQLOperator(BaseOperator):
    """
    Executes sql code in a specific Microsoft SQL database

    :param mssql_conn_id    : reference to a specific mssql database
    :param sql              : the sql code to be executed
    :param database         : name of database which overwrite defined one in connection
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
        self,
        sql: str,
        mssql_conn_id='mssql_default',
        parameters: dict = None,
        autocommit: bool = False,
        database: str = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.mssql_conn_id = mssql_conn_id
        self.sql = sql
        self.parameters = parameters
        self.autocommit = autocommit
        self.database = database

    def execute(self, context):
        """
        Execute the sql code in a Microsoft SQL database
        """
        logging.info('Executing: %s', self.sql)
        hook = MSSQLHook(
            mssql_conn_id=self.mssql_conn_id,
            schema=self.database,
        )
        hook.run(
            self.sql,
            autocommit=self.autocommit,
            parameters=self.parameters,
        )
