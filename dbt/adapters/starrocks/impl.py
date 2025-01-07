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

from concurrent.futures import Future
from typing import Callable, Dict, List, Optional, Set, FrozenSet, Tuple, Any

import agate
import dbt.exceptions
from dbt.adapters.base import available
from dbt.adapters.base.impl import _expect_row_value, catch_as_completed
from dbt.adapters.base.relation import InformationSchema
from dbt.adapters.protocol import AdapterConfig
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.sql.impl import LIST_RELATIONS_MACRO_NAME, LIST_SCHEMAS_MACRO_NAME
from dbt_common.clients.agate_helper import table_from_rows
from dbt.contracts.graph.manifest import Manifest
from dbt.adapters.contracts.relation import RelationType
from dbt_common.utils import executor

from dbt.adapters.starrocks.column import StarRocksColumn
from dbt.adapters.starrocks.connections import StarRocksConnectionManager
from dbt.adapters.starrocks.relation import StarRocksRelation


class StarRocksConfig(AdapterConfig):
    engine: Optional[str] = None
    table_type: Optional[str] = None  # DUPLICATE/PRIMARY/UNIQUE/AGGREGATE
    keys: Optional[List[str]] = None
    partition_by: Optional[List[str]] = None
    partition_by_init: Optional[List[str]] = None
    distributed_by: Optional[List[str]] = None
    buckets: Optional[int] = None
    properties: Optional[Dict[str, str]] = None
    materialized: Optional[str] = None  # table/view/materialized_view


class StarRocksAdapter(SQLAdapter):
    ConnectionManager = StarRocksConnectionManager
    Relation = StarRocksRelation
    AdapterSpecificConfigs = StarRocksConfig
    Column = StarRocksColumn

    @classmethod
    def date_function(cls) -> str:
        return "current_date()"

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "datetime"

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "string"

    def quote(self, identifier):
        return "`{}`".format(identifier)

    def check_schema_exists(self, database, schema):
        results = self.execute_macro(
            LIST_SCHEMAS_MACRO_NAME, kwargs={"database": database}
        )

        exists = True if schema in [row[0] for row in results] else False
        return exists

    def get_relation(self, database: Optional[str], schema: str, identifier: str):
        if not self.Relation.get_default_include_policy().database:
            database = None

        return super().get_relation(database, schema, identifier)

    def list_relations_without_caching(
        self, schema_relation: StarRocksRelation
    ) -> List[StarRocksRelation]:
        kwargs = {"schema_relation": schema_relation}
        results = self.execute_macro(LIST_RELATIONS_MACRO_NAME, kwargs=kwargs)

        relations = []
        for row in results:
            if len(row) != 4:
                raise dbt.exceptions.DbtRuntimeError(
                    f"Invalid value from 'show table extended ...', "
                    f"got {len(row)} values, expected 4"
                )
            _database, name, schema, type_info = row
            relation = self.Relation.create(
                database=None,
                schema=schema,
                identifier=name,
                type=self.Relation.get_relation_type(type_info),
            )
            relations.append(relation)

        return relations

    def drop_relation(self, relation: StarRocksRelation) -> None:
        """Drop the given relation and its dependencies (like views)."""
        if relation.type == RelationType.View:
            sql = "DROP VIEW IF EXISTS {relation}"
        else:
            sql = "DROP TABLE IF EXISTS {relation}"
        self.execute_macro(
            'drop_relation',
            kwargs={'relation': relation, 'sql': sql}
        )

    def truncate_relation(self, relation: StarRocksRelation) -> None:
        """Truncate the given relation."""
        sql = "TRUNCATE TABLE {relation}"
        self.execute_macro(
            'truncate_relation',
            kwargs={'relation': relation, 'sql': sql}
        )

    def rename_relation(
        self, from_relation: StarRocksRelation, to_relation: StarRocksRelation
    ) -> None:
        """Rename the relation from one name to another."""
        self.execute_macro(
            'rename_relation',
            kwargs={
                'from_relation': from_relation,
                'to_relation': to_relation
            }
        )

    def materialize_as_table(
        self, dataset: agate.Table, table: StarRocksRelation
    ) -> None:
        """Convert the given dataset into a table."""
        if len(dataset.rows) == 0:
            return

        column_names = dataset.column_names
        quoted_columns = [self.quote(col) for col in column_names]
        column_list = ", ".join(quoted_columns)
        value_list = []
        
        for row in dataset.rows:
            values = []
            for value in row:
                if value is None:
                    values.append('NULL')
                elif isinstance(value, str):
                    values.append("'{}'".format(value.replace("'", "''")))
                else:
                    values.append(str(value))
            value_list.append("({})".format(", ".join(values)))

        sql = """
            INSERT INTO {table} ({columns})
            VALUES {values}
        """.format(
            table=table,
            columns=column_list,
            values=",\n".join(value_list)
        )
        self.execute(sql)

    def create_view_as(self, relation: StarRocksRelation, sql: str) -> None:
        """Create a view."""
        sql = f"CREATE VIEW {relation} AS {sql}"
        self.execute(sql)

    def create_materialized_view(
        self, relation: StarRocksRelation, sql: str, config: Dict[str, Any]
    ) -> None:
        """Create a materialized view with the given properties."""
        properties = []
        
        if config.get('refresh_type'):
            properties.append(f"REFRESH {config['refresh_type']}")
        
        if config.get('partition_by'):
            partition_cols = ", ".join(config['partition_by'])
            properties.append(f"PARTITION BY ({partition_cols})")
        
        if config.get('distributed_by'):
            dist_cols = ", ".join(config['distributed_by'])
            properties.append(f"DISTRIBUTED BY HASH({dist_cols})")
            
        if config.get('buckets'):
            properties.append(f"BUCKETS {config['buckets']}")
            
        properties_str = " ".join(properties)
        
        sql = f"CREATE MATERIALIZED VIEW {relation} {properties_str} AS {sql}"
        self.execute(sql)

    def get_catalog(self, manifest, used_schemas):
        schema_map = self._get_catalog_schemas(manifest)
        if len(schema_map) > 1:
            dbt.exceptions.CompilationError(
                f"Expected only one database in get_catalog, found "
                f"{list(schema_map)}"
            )

        with executor(self.config) as tpe:
            futures: List[Future[agate.Table]] = []
            for info, schemas in schema_map.items():
                for schema in schemas:
                    for d, s in used_schemas:
                        if schema.lower() == s.lower():
                            schema = s
                            break
                    futures.append(
                        tpe.submit_connected(
                            self,
                            schema,
                            self._get_one_catalog,
                            info,
                            [schema],
                            used_schemas,
                        )
                    )
            catalogs, exceptions = catch_as_completed(futures)
        return catalogs, exceptions

    @classmethod
    def _catalog_filter_table(
        cls, table: "agate.Table", used_schemas: FrozenSet[Tuple[str, str]]
    ) -> agate.Table:
        table = table_from_rows(
            table.rows,
            table.column_names,
            text_only_columns=["table_schema", "table_name"],
        )
        return table.where(_catalog_filter_schemas(used_schemas))

    @available
    def is_before_version(self, version: str) -> bool:
        conn = self.connections.get_if_exists()
        if conn:
            server_version = conn.handle.server_version
            server_version_tuple = tuple(server_version)
            version_detail_tuple = tuple(
                int(part) for part in version.split(".") if part.isdigit())
            if version_detail_tuple > server_version_tuple:
                return True
        return False

    @available
    def current_version(self):
        conn = self.connections.get_if_exists()
        if conn:
            server_version = conn.handle.server_version
            if server_version != (999, 999, 999):
                return "{}.{}.{}".format(server_version[0], server_version[1], server_version[2])
        return 'UNKNOWN'

    def _get_one_catalog(
        self,
        information_schema: InformationSchema,
        schemas: Set[str],
        used_schemas: FrozenSet[Tuple[str, str]],
    ) -> agate.Table:
        if len(schemas) != 1:
            dbt.exceptions.CompilationError(
                f"Expected only one schema in StarRocks _get_one_catalog, found "
                f"{schemas}"
            )

        return super()._get_one_catalog(information_schema, schemas, used_schemas)


def _catalog_filter_schemas(used_schemas: FrozenSet[Tuple[str, str]]) -> Callable[[agate.Row], bool]:
    schemas = frozenset((None, s.lower()) for d, s in used_schemas)

    def test(row: agate.Row) -> bool:
        table_database = _expect_row_value("table_database", row)
        table_schema = _expect_row_value("table_schema", row)
        if table_schema is None:
            return False
        return (table_database, table_schema.lower()) in schemas

    return test
