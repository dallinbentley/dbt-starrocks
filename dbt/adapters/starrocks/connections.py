#! /usr/bin/python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from contextlib import contextmanager

import mysql.connector

import dbt.exceptions
import dbt_common.exceptions
from dataclasses import dataclass

from dbt.adapters.contracts.connection import (
    Credentials,
    AdapterResponse,
    Connection
)
from dbt.adapters.sql import SQLConnectionManager
from dbt.adapters.events.logging import AdapterLogger
from typing import Optional

logger = AdapterLogger("starrocks")


@dataclass
class StarRocksCredentials(Credentials):
    # Required fields (no defaults)
    host: str
    username: str
    password: str
    database: str  # Required by dbt core
    # Optional fields (with defaults)
    port: int
    catalog: str
    charset: Optional[str]
    schema: Optional[str]
    version: Optional[str]
    use_pure: bool

    @property
    def unique_field(self) -> str:
        """Return the field that uniquely identifies this connection."""
        return self.host

    def __post_init__(self):
        # Use database value as schema if schema not provided
        self.schema = self.schema or self.database

    @property
    def type(self):
        return 'starrocks'

    def _connection_keys(self):
        """
        Returns an iterator of keys to pretty-print in 'dbt debug'
        """
        return (
            "host",
            "port",
            "catalog",
            "schema",
            "username",
        )


def _parse_version(result):
    default_version = (999, 999, 999)
    first_part = None

    if '-' in result:
        first_part = result.split('-')[0]
    if ' ' in result:
        first_part = result.split(' ')[0]

    if first_part and len(first_part.split('.')) == 3:
        return int(first_part[0]), int(first_part[2]), int(first_part[4])

    return default_version


class StarRocksConnectionManager(SQLConnectionManager):
    TYPE = 'starrocks'

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        credentials = cls.get_credentials(connection.credentials)
        kwargs = {
            "host": credentials.host,
            "port": credentials.port,
            "user": credentials.username,
            "password": credentials.password,
            "database": f"{credentials.catalog}.{credentials.schema}",
            "charset": credentials.charset,
            "use_pure": credentials.use_pure,
            "buffered": True,
            "connection_timeout": 60
        }

        try:
            connection.handle = mysql.connector.connect(**kwargs)
            connection.state = 'open'
            
            # Add server_version handling
            cursor = connection.handle.cursor()
            cursor.execute("SELECT version()")
            version_str = cursor.fetchone()[0]
            cursor.close()
            connection.handle.server_version = _parse_version(version_str)
            
        except mysql.connector.Error as e:
            if e.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
                try:
                    # Try connecting to information_schema to create the database
                    kwargs["database"] = f"{credentials.catalog}.information_schema"
                    temp_conn = mysql.connector.connect(**kwargs)
                    cursor = temp_conn.cursor()
                    
                    # Create the database if it doesn't exist
                    create_db_sql = f"CREATE DATABASE IF NOT EXISTS `{credentials.catalog}`.`{credentials.schema}`"
                    cursor.execute(create_db_sql)
                    cursor.close()
                    temp_conn.close()

                    # Reconnect with the new database
                    kwargs["database"] = f"{credentials.catalog}.{credentials.schema}"
                    connection.handle = mysql.connector.connect(**kwargs)
                    connection.state = 'open'
                except mysql.connector.Error as create_err:
                    logger.debug(f"Failed to create database: {str(create_err)}")
                    connection.handle = None
                    connection.state = 'fail'
                    raise dbt_common.exceptions.FailedToConnectError(str(create_err))
            else:
                logger.debug(f"Error connecting to StarRocks: {str(e)}")
                connection.handle = None
                connection.state = 'fail'
                raise dbt_common.exceptions.FailedToConnectError(str(e))

        return connection

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    def cancel(self, connection: Connection):
        connection.handle.close()

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except mysql.connector.DatabaseError as e:
            logger.debug('StarRocks error: {}'.format(str(e)))

            try:
                self.rollback_if_open()
            except mysql.connector.Error:
                logger.debug("Failed to release connection!")
                pass

            raise dbt_common.exceptions.DbtDatabaseError(str(e).strip()) from e

        except Exception as e:
            logger.debug("Error running SQL: {}", sql)
            logger.debug("Rolling back transaction.")
            self.rollback_if_open()
            if isinstance(e, dbt.exceptions.DbtRuntimeError):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise

            raise dbt_common.exceptions.DbtRuntimeError(str(e)) from e

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:
        code = "SUCCESS"
        num_rows = 0

        if cursor is not None and cursor.rowcount is not None:
            num_rows = cursor.rowcount

        # There's no real way to get the status from the mysql-connector-python driver.
        # So just return the default value.
        return AdapterResponse(
            _message="{} {}".format(code, num_rows),
            rows_affected=num_rows,
            code=code
        )
