from __future__ import annotations

import io
import logging
import sys
from collections.abc import Mapping
from typing import Any

import pandas as pd
from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    Integer,
    String,
    create_engine,
    inspect,
    text,
)
from sqlalchemy.engine import URL, Engine

from airflow_toolkit._compact.airflow_shim import (
    BaseHook,
    BaseOperator,
    Connection,
    Context,
)
from airflow_toolkit.filesystems.filesystem_factory import FilesystemFactory
from airflow_toolkit.filesystems.filesystem_protocol import FilesystemProtocol

if sys.version_info < (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

logger = logging.getLogger(__name__)


type_mapping: dict[str, type[Any]] = {
    "int64": Integer,
    "int": Integer,
    "integer": Integer,
    "float64": Float,
    "float": Float,
    "object": String,
    "string": String,
    "str": String,
    "datetime64[ns]": DateTime,
    "bool": Boolean,
    "boolean": Boolean,
}


class FilesystemToDatabaseOperator(BaseOperator):
    """
    This operator will copy a file from a filesystem to a database table.
    """

    template_fields = (
        "db_table",
        "db_schema",
        "filesystem_conn_id",
        "database_conn_id",
        "filesystem_path",
        "metadata",
    )

    def __init__(
        self,
        filesystem_conn_id: str,
        database_conn_id: str,
        filesystem_path: str,
        db_table: str,
        db_schema: str | None = None,
        source_format: Literal["csv", "json", "parquet"] = "csv",
        source_format_options: Mapping[str, Any] | None = None,
        table_aggregation_type: Literal["append", "fail", "replace"] = "append",
        metadata: Mapping[str, str] | None = None,
        metadata_columns_in_uppercase: bool = True,
        include_source_path: bool = True,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.filesystem_conn_id = filesystem_conn_id
        self.database_conn_id = database_conn_id
        self.filesystem_path = filesystem_path
        self.db_table = db_table
        self.db_schema = db_schema
        self.source_format = source_format
        self.source_format_options: dict[str, Any] = (
            dict(source_format_options) if source_format_options is not None else {}
        )
        self.table_aggregation_type = table_aggregation_type
        self.metadata: dict[str, str] = (
            dict(metadata) if metadata is not None else {"_DS": "{{ ds }}"}
        )
        self.metadata_columns_in_uppercase = metadata_columns_in_uppercase
        self.include_source_path = include_source_path

    def execute(self, context: Context) -> None:
        logger.info(f"Create connection for filesystem ({self.filesystem_conn_id})")
        filesystem: FilesystemProtocol = FilesystemFactory.get_data_lake_filesystem(
            connection=BaseHook.get_connection(self.filesystem_conn_id),
        )
        logger.info(
            f"Create SQLAlchemy engine with connection_id {self.database_conn_id}"
        )
        conn: Connection = BaseHook.get_connection(self.database_conn_id)

        if conn.conn_type == "sqlite":
            # In your fixture, the DB path is stored in `host`
            url = URL.create(
                "sqlite",
                database=conn.host,  # absolute path, e.g. /tmp/pytest-.../test_db.sqlite
            )
        else:
            # Generic case (Postgres, MySQL, etc.)
            url = URL.create(
                conn.conn_type,  # e.g. "postgresql"
                username=conn.login,
                password=conn.password,  # may be None → handled safely
                host=conn.host,
                port=conn.port,
                database=conn.schema,
            )

        engine: Engine = create_engine(url)

        # TODO: check prefix argument in filesystem.list_files
        for blob_path in filesystem.list_files(""):
            if not blob_path.startswith(self.filesystem_path):
                logger.warning(
                    f"Blob {blob_path} is not under the expected prefix {self.filesystem_path}. Skipping..."
                )
                continue

            # Check the file extension is one of accepted,
            # TODO: make it extensible for all format we accept
            # if not blob_path.endswith(
            #     (f".{self.source_format}", f".{self.source_format}.gz")
            # ):
            #     logger.warning(
            #         f"Blob {blob_path} is not in the right format. Skipping..."
            #     )
            #     continue

            logger.info(f"Read file {blob_path} and convert to pandas")
            raw_content = io.BytesIO(filesystem.read(blob_path))

            df = self.raw_content_to_pandas(path_or_buf=raw_content)

            self._check_and_fix_column_differences(df, self.db_table, engine)

            for key, value in self.metadata.items():
                metadata_key = (
                    key.upper() if self.metadata_columns_in_uppercase else key.lower()
                )

                df[metadata_key] = value

                if metadata_key.startswith("_"):
                    # _* fields are treated as metadata and we try to convert them in datetimes
                    df[metadata_key] = self._convert_to_datetime(df[metadata_key])

            if self.include_source_path:
                source_path_column = (
                    "_LOADED_FROM"
                    if self.metadata_columns_in_uppercase
                    else "_loaded_from"
                )
                df[source_path_column] = blob_path
                df[source_path_column] = df[source_path_column].astype("string")

            df.to_sql(
                name=self.db_table,
                schema=self.db_schema,
                con=engine,
                if_exists=self.table_aggregation_type,
                index=False,
            )

    def _check_and_fix_column_differences(
        self, df: pd.DataFrame, table_name: str, engine: Engine
    ) -> None:
        """
        This method checks if the columns in the dataframe are the same as the
        ones in the database table. If they are not, it adds the missing
        columns to the database table or fills them with None.

        Args:
            df (pd.DataFrame): dataframe to be inserted into the database
            table_name (str): name of the database table
            engine (engine.Engine): SQLAlchemy engine object for the database connection
        """
        inspector = inspect(engine)

        if table_name in inspector.get_table_names():
            source_file_columns = set(df.columns.tolist())
            table_columns = {
                col["name"]
                for col in inspector.get_columns(self.db_table)
                if col["name"] not in self.metadata
            }

            # Column in source file but not present in db table
            only_csv_columns = source_file_columns - table_columns
            with engine.connect() as conn:
                for column in only_csv_columns:
                    logger.warning(
                        f'Table "{table_name}" does not have column "{column}" present in the source file, '
                        f'column "{column}" will be filled with null values.'
                    )
                    dtype_name = str(df.dtypes.get(column, ""))
                    sqlalchemy_type = type_mapping.get(dtype_name, String)
                    conn.execute(
                        text(
                            f'ALTER TABLE {table_name} ADD COLUMN "{column}" {sqlalchemy_type.__name__}'
                        )
                    )

            # Column in db table but not present in the source file
            only_db_table_columns = table_columns - source_file_columns
            for column in only_db_table_columns:
                logger.warning(
                    f'Source file does not have column "{column}" present in the table "{table_name}", '
                    f'column "{column}" will be filled with null values.'
                )
                df[column] = None

    @staticmethod
    def _convert_to_datetime(value: pd.Series) -> pd.Series:
        try:
            return pd.to_datetime(value)
        except (ValueError, TypeError):
            return value

    def raw_content_to_pandas(
        self, path_or_buf: str | bytes | io.StringIO | io.BytesIO
    ) -> pd.DataFrame:
        options: dict[str, Any] = dict(self.source_format_options)

        match self.source_format:
            case "csv":
                return pd.read_csv(path_or_buf, **options)
            case "json":
                return pd.read_json(path_or_buf, **options)
            case "parquet":
                return pd.read_parquet(path_or_buf, **options)
            case _:
                raise ValueError(f"Unknown source format {self.source_format}")
