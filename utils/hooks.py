"""
Helper module for creating a custom airflow hook for a MSSQL database
"""
from __future__ import annotations

import pyodbc
from airflow.hooks.dbapi_hook import DbApiHook


class MSSQLHook(DbApiHook):
    """
    Interact with Microsoft SQL Server.
    """

    conn_name_attr = 'mssql_conn_id'
    default_conn_name = 'mssql_default'
    supports_autocommit = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schema = kwargs.pop('schema', None)
        self.conn = None

    def get_conn(self):
        """
        Returns a mssql connection object
        """
        if self.conn:
            return self.conn

        conn = self.get_connection(self.mssql_conn_id)
        conn_str = (
            'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={0};'
            + 'PORT={1};DATABASE={2};UID={3};PWD={4}'.format(  # noqa: F523
                conn.host, conn.port, conn.schema, conn.login, conn.password,
            )
        )
        self.conn = pyodbc.connect(conn_str)
        return self.conn

    def set_autocommit(self, conn, autocommit):
        conn.autocommit = autocommit
