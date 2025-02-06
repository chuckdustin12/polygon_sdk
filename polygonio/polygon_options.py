#!/usr/bin/env python3
"""
Module: polygon_options.py
--------------------------
This module provides a class (PolygonOptions) for integrating with the Polygon.io API.
It includes methods for fetching, filtering, paginating, and saving option data into a PostgreSQL database
using asyncpg and asynchronous HTTP clients (aiohttp and httpx). In addition, the module contains many helper functions
for data formatting, flattening nested dictionaries, timestamp conversions, XML parsing, plotting, and more.

Configuration:
    - Ensure your environment variables are set (e.g., via a .env file) with:
          YOUR_POLYGON_KEY
    - Required packages:
          aiohttp, asyncpg, pandas, numpy, pytz, httpx, plotly, kaleido, beautifulsoup4, more_itertools, python-dotenv, tabulate
    - Install dependencies with:
          pip install aiohttp asyncpg pandas numpy pytz httpx plotly kaleido beautifulsoup4 more_itertools python-dotenv tabulate

Usage:
    Import and instantiate the PolygonOptions class. Use the async methods within an asyncio event loop:
        >>> import asyncio
        >>> from polygon_options import PolygonOptions
        >>> async def main():
        ...     poly = PolygonOptions()
        ...     await poly.connect()
        ...     # Fetch universal snapshot for a ticker:
        ...     snapshot = await poly.get_universal_snapshot("AAPL")
        ...     print(snapshot)
        ...     await poly.close()
        >>> asyncio.run(main())
"""

import os
import time
import sys
import re
import pytz
import httpx
import aiohttp
from typing import List, AsyncGenerator, Tuple, Optional, Dict, Any, Union
import asyncpg
import logging
import numpy as np
import pandas as pd
import asyncio
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode
from asyncio import Lock
from tabulate import tabulate
from dotenv import load_dotenv
from more_itertools import chunked
from asyncpg.exceptions import UniqueViolationError

# Local imports from your codebase
from .polygon_helpers import to_datetime_eastern
from .mapping import OPTIONS_EXCHANGES
from ._markets.list_sets.dicts import option_conditions
from .polygon_helpers import chunk_string

# Models
from .models.technicals import RSI
from .models.option_models.option_snapshot import WorkingUniversal
from .models.option_models.universal_snapshot import (
    UniversalOptionSnapshot,
    UniversalOptionSnapshot2,
    SpxSnapshot
)
from .polygon_helpers import (
    get_human_readable_string,
    flatten_nested_dict,
    flatten_dict
)

load_dotenv()

# Global lock and semaphore for concurrency and race-condition prevention.
lock: Lock = Lock()
sema: asyncio.Semaphore = asyncio.Semaphore(4)

# ---------------------- Helper Functions ---------------------- #
def dtype_to_postgres(dtype: Any) -> str:
    """
    Map a Pandas dtype to a PostgreSQL column type.
    """
    if pd.api.types.is_integer_dtype(dtype):
        return 'INTEGER'
    elif pd.api.types.is_float_dtype(dtype):
        return 'REAL'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'TIMESTAMP'
    elif pd.api.types.is_string_dtype(dtype):
        return 'TEXT'
    else:
        return 'TEXT'

def convert_ns_to_est(ns_timestamp: Union[int, float]) -> str:
    """
    Convert a Unix nanosecond timestamp to an Eastern Standard Time (EST) date string.
    """
    timestamp_in_seconds = ns_timestamp / 1e9
    utc_time = datetime.fromtimestamp(timestamp_in_seconds, tz=timezone.utc)
    est_time = utc_time.astimezone(pytz.timezone('US/Eastern')).replace(tzinfo=None)
    return est_time.strftime("%Y-%m-%d %H:%M:%S")

async def timed_safe(coro: Any, timeout: int = 10) -> Tuple[Any, float, Optional[Exception]]:
    """
    Run a coroutine with a timeout and return a tuple of (result, elapsed time, error).
    """
    start = time.perf_counter()
    try:
        result = await asyncio.wait_for(coro, timeout=timeout)
        elapsed = time.perf_counter() - start
        return result, elapsed, None
    except Exception as e:
        elapsed = time.perf_counter() - start
        return None, elapsed, e

async def sem_timed(coro: Any, semaphore: asyncio.Semaphore, timeout: int = 10) -> Tuple[Any, float, Optional[Exception]]:
    """
    Run a coroutine under a semaphore with a timeout.
    """
    async with semaphore:
        return await timed_safe(coro, timeout=timeout)

def inspect_object(obj: Any) -> Any:
    """
    Return an object's __dict__ or slots as a dictionary.
    """
    if hasattr(obj, "__dict__"):
        return obj.__dict__
    elif hasattr(obj, "__slots__"):
        return {slot: getattr(obj, slot) for slot in obj.__slots__}
    else:
        return str(obj)

# ---------------------- PolygonOptions Class ---------------------- #
class PolygonOptions:
    """
    Provides integration with the Polygon.io API for fetching, filtering,
    and saving option data into a PostgreSQL database via asyncpg and HTTP clients.
    """

    def __init__(
        self,
        user: str = 'chuck',
        database: str = 'fudstop3',
        host: str = 'localhost',
        port: int = 5432,
        password: str = 'fud'
    ) -> None:
        """
        Initialize database connection parameters and common date references.
        """
        self.user = user
        self.database = database
        self.host = host
        self.port = port
        self.password = password

        self.conn = None
        self.pool = None
        self.session = None
        self.http_session = None  # For aiohttp

        self.api_key: str = os.environ.get('YOUR_POLYGON_KEY', '')
        if not self.api_key:
            logging.warning("No Polygon API key found in environment.")

        now = datetime.now()
        self.today = now.strftime('%Y-%m-%d')
        self.yesterday = (now - timedelta(days=1)).strftime('%Y-%m-%d')
        self.tomorrow = (now + timedelta(days=1)).strftime('%Y-%m-%d')
        self.thirty_days_ago = (now - timedelta(days=30)).strftime('%Y-%m-%d')
        self.thirty_days_from_now = (now + timedelta(days=30)).strftime('%Y-%m-%d')
        self.fifteen_days_ago = (now - timedelta(days=15)).strftime('%Y-%m-%d')
        self.fifteen_days_from_now = (now + timedelta(days=15)).strftime('%Y-%m-%d')
        self.eight_days_ago = (now - timedelta(days=8)).strftime('%Y-%m-%d')
        self.eight_days_from_now = (now + timedelta(days=8)).strftime('%Y-%m-%d')
        self.one_year_from_now = (now + timedelta(days=365)).strftime('%Y-%m-%d')
        self.one_year_ago = (now - timedelta(days=365)).strftime('%Y-%m-%d')
        self._45_days_from_now = (now + timedelta(days=45)).strftime('%Y-%m-%d')
        self._90_days_drom_now = (now + timedelta(days=90)).strftime('%Y-%m-%d')

        self.connection_string = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        self.db_config = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.database
        }

    # ------------------ AIOHTTP Session Handling ------------------ #
    async def __aenter__(self) -> "PolygonOptions":
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        if self.session:
            await self.session.close()

    async def create_http_session(self) -> aiohttp.ClientSession:
        """
        Create an aiohttp session if needed.
        """
        if self.http_session is None or self.http_session.closed:
            self.http_session = aiohttp.ClientSession()
        return self.http_session

    async def get_http_session(self) -> aiohttp.ClientSession:
        """
        Return the existing aiohttp session or create a new one.
        """
        if self.http_session and not self.http_session.closed:
            return self.http_session
        return await self.create_http_session()

    async def close_http_session(self) -> None:
        """
        Close the aiohttp session.
        """
        if self.http_session and not self.http_session.closed:
            await self.http_session.close()
            self.http_session = None

    # ------------------ PostgreSQL Connection Handling ------------------ #
    async def connect(self) -> asyncpg.Pool:
        """
        Initialize the database connection pool if not already present.
        """
        if not self.pool:
            self.pool = await asyncpg.create_pool(
                host=self.host,
                user=self.user,
                port=self.port,
                database=self.database,
                password=self.password,
                min_size=1,
                max_size=10
            )
        return self.pool

    async def close(self) -> None:
        """
        Close the database connection pool.
        """
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def disconnect(self) -> None:
        """
        Alias for close.
        """
        await self.close()

    async def fetch_new(self, query: str, *args: Any) -> List[asyncpg.Record]:
        """
        Fetch rows from the database with a given query.
        """
        async with self.pool.acquire() as connection:
            return await connection.fetch(query, *args)

    async def execute(self, query: str, *args: Any) -> str:
        """
        Execute a non-returning SQL query.
        """
        async with self.pool.acquire() as connection:
            return await connection.execute(query, *args)

    async def fetch(self, query: str) -> List[asyncpg.Record]:
        """
        Convenience method to fetch all rows from the database.
        """
        try:
            async with self.pool.acquire() as conn:
                records = await conn.fetch(query)
                return records
        except Exception as e:
            logging.error(e)
            return []

    async def table_exists(self, table_name: str) -> bool:
        """
        Check whether a table exists in the database.
        """
        query = f"SELECT to_regclass('{table_name}')"
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                exists = await conn.fetchval(query)
        return exists is not None

    # ------------------ HTTP / Data Fetching Utilities ------------------ #
    async def fetch_page(self, url: str) -> Dict[str, Any]:
        """
        Fetch a single page from the given URL using aiohttp.
        """
        session = await self.get_http_session()
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            logging.error(f"Error fetching {url} - {e}")
            return {}

    async def paginate_concurrent(self, url: str, as_dataframe: bool = False, concurrency: int = 25) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
        """
        Concurrently paginate through endpoints (using 'next_url') and collect all results.
        """
        if "apiKey" not in url:
            delimiter = '&' if '?' in url else '?'
            url = f"{url}{delimiter}apiKey={self.api_key}"

        all_results: List[Dict[str, Any]] = []
        pages_to_fetch: List[str] = [url]

        while pages_to_fetch:
            tasks = []
            for _ in range(min(concurrency, len(pages_to_fetch))):
                next_url = pages_to_fetch.pop(0)
                tasks.append(self.fetch_page(next_url))
            results = await asyncio.gather(*tasks)
            if results:
                for data in results:
                    if data and isinstance(data, dict):
                        if "results" in data:
                            all_results.extend(data["results"])
                        next_url = data.get("next_url")
                        if next_url:
                            delimiter = '&' if '?' in next_url else '?'
                            next_url = f"{next_url}{delimiter}apiKey={self.api_key}"
                            pages_to_fetch.append(next_url)
        if as_dataframe:
            return pd.DataFrame(all_results)
        return all_results

    # ------------------ Database Table Creation & Batch Insert/Upsert ------------------ #
    async def create_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        unique_column: str,
        create_history: bool = False
    ) -> None:
        """
        Create a table based on a DataFrame schema if it does not exist.
        Optionally, create a history table with triggers.
        """
        logging.info(f"Creating table '{table_name}' if not exists...")

        dtype_mapping = {
            'int64': 'INTEGER',
            'float64': 'FLOAT',
            'object': 'TEXT',
            'bool': 'BOOLEAN',
            'datetime64': 'TIMESTAMP',
            'datetime64[ns]': 'TIMESTAMP',
            'datetime64[ms]': 'TIMESTAMP',
            'datetime64[ns, US/Eastern]': 'TIMESTAMP',
            'string': 'TEXT',
            'int32': 'INTEGER',
            'float32': 'FLOAT',
            'datetime64[us]': 'TIMESTAMP',
            'timedelta[ns]': 'INTERVAL',
            'category': 'TEXT',
            'int16': 'SMALLINT',
            'int8': 'SMALLINT',
            'uint8': 'SMALLINT',
            'uint16': 'INTEGER',
            'uint32': 'BIGINT',
            'uint64': 'NUMERIC',
            'complex64': 'COMPLEX',
            'complex128': 'COMPLEX',
            'bytearray': 'BYTEA',
            'bytes': 'BYTEA',
            'memoryview': 'BYTEA',
            'list': 'INTEGER[]'
        }

        # Adjust mapping for int64 if necessary
        for col, col_dtype in zip(df.columns, df.dtypes):
            if col_dtype == 'int64':
                max_val = df[col].max()
                min_val = df[col].min()
                if max_val > 2**31 - 1 or min_val < -2**31:
                    dtype_mapping['int64'] = 'BIGINT'

        history_table_name = f"{table_name}_history"

        async with self.pool.acquire() as connection:
            table_exists = await connection.fetchval(f"SELECT to_regclass('{table_name}')")
            if table_exists is None:
                if isinstance(unique_column, str):
                    unique_column = [col.strip() for col in unique_column.split(",")]
                unique_constraint = f'UNIQUE ({", ".join(unique_column)})' if unique_column else ''
                create_query = f"""
                CREATE TABLE {table_name} (
                    {', '.join(f'"{col}" {dtype_mapping.get(str(dtype), "TEXT")}'
                    for col, dtype in zip(df.columns, df.dtypes))},
                    "insertion_timestamp" TIMESTAMP,
                    {unique_constraint}
                )
                """
                try:
                    await connection.execute(create_query)
                    logging.info(f"Table {table_name} created successfully.")
                except asyncpg.UniqueViolationError as e:
                    logging.error(f"Unique violation error: {e}")
            else:
                logging.info(f"Table {table_name} already exists.")

            if create_history:
                history_create_query = f"""
                CREATE TABLE IF NOT EXISTS {history_table_name} (
                    operation CHAR(1) NOT NULL,
                    changed_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
                    {', '.join(f'"{col}" {dtype_mapping.get(str(dtype), "TEXT")}'
                    for col, dtype in zip(df.columns, df.dtypes))}
                );
                """
                await connection.execute(history_create_query)
                trigger_function_query = f"""
                CREATE OR REPLACE FUNCTION save_to_{history_table_name}()
                RETURNS TRIGGER AS $$
                BEGIN
                    INSERT INTO {history_table_name} (
                        operation, changed_at, {', '.join(f'"{col}"' for col in df.columns)}
                    )
                    VALUES (
                        CASE WHEN (TG_OP = 'DELETE') THEN 'D'
                             WHEN (TG_OP = 'UPDATE') THEN 'U'
                             ELSE 'I' END,
                        current_timestamp,
                        {', '.join('OLD.' + f'"{col}"' for col in df.columns)}
                    );
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
                """
                await connection.execute(trigger_function_query)
                trigger_query = f"""
                DROP TRIGGER IF EXISTS tr_{history_table_name} ON {table_name};
                CREATE TRIGGER tr_{history_table_name}
                AFTER UPDATE OR DELETE ON {table_name}
                FOR EACH ROW EXECUTE FUNCTION save_to_{history_table_name}();
                """
                await connection.execute(trigger_query)
                logging.info(f"History table {history_table_name} and trigger created successfully.")

            for col, col_dtype in zip(df.columns, df.dtypes):
                alter_query = f"""
                DO $$
                BEGIN
                    BEGIN
                        ALTER TABLE {table_name} ADD COLUMN "{col}" {dtype_mapping.get(str(col_dtype), "TEXT")};
                    EXCEPTION
                        WHEN duplicate_column THEN
                            NULL;
                    END;
                END $$;
                """
                await connection.execute(alter_query)

    def sanitize_value(self, value: Any, col_type: str) -> str:
        """
        Sanitize a value for SQL queries based on the column type.
        """
        if col_type == 'str':
            return f"'{value}'"
        elif col_type == 'date':
            if isinstance(value, str):
                try:
                    datetime.strptime(value, '%Y-%m-%d')
                    return f"'{value}'"
                except ValueError:
                    raise ValueError(f"Invalid date format: {value}")
            elif isinstance(value, datetime):
                return f"'{value.strftime('%Y-%m-%d')}'"
        else:
            return str(value)

    async def batch_insert_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        unique_columns: Union[str, List[str]],
        batch_size: int = 250
    ) -> None:
        """
        Batch insert a DataFrame into a table. On conflict, update non-unique columns.
        """
        try:
            async with lock:
                if not await self.table_exists(table_name):
                    await self.create_table(df, table_name, unique_columns)
                df = df.copy()
                df['insertion_timestamp'] = pd.to_datetime([datetime.now() for _ in range(len(df))])
                records = df.to_dict(orient='records')
                async with self.pool.acquire() as connection:
                    column_types = await connection.fetch(
                        f"""
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_name = '{table_name}'
                        """
                    )
                    type_mapping = {col['column_name']: col['data_type'] for col in column_types}
                    if isinstance(unique_columns, str):
                        unique_columns = [uc.strip() for uc in unique_columns.split(",")]
                    insert_query = f"""
                    INSERT INTO {table_name} (
                        {', '.join(f'"{col}"' for col in df.columns)}
                    )
                    VALUES (
                        {', '.join('$' + str(i) for i in range(1, len(df.columns) + 1))}
                    )
                    ON CONFLICT ({', '.join(unique_columns)})
                    DO UPDATE SET {', '.join(f'"{col}" = excluded."{col}"'
                    for col in df.columns if col not in unique_columns)}
                    """
                    batch_data = []
                    for record in records:
                        new_record = []
                        for col, val in record.items():
                            pg_type = type_mapping.get(col)
                            if pd.isna(val):
                                new_record.append(None)
                            elif pg_type in ['timestamp', 'timestamp without time zone', 'timestamp with time zone']:
                                if isinstance(val, np.datetime64):
                                    new_record.append(pd.Timestamp(val).to_pydatetime().replace(tzinfo=None))
                                elif isinstance(val, datetime):
                                    new_record.append(val)
                            elif pg_type in ['double precision', 'real'] and not isinstance(val, str):
                                new_record.append(float(val))
                            elif isinstance(val, np.int64):
                                new_record.append(int(val))
                            elif pg_type == 'integer' and not isinstance(val, int):
                                new_record.append(int(val))
                            else:
                                new_record.append(val)
                        batch_data.append(tuple(new_record))
                        if len(batch_data) == batch_size:
                            try:
                                await connection.executemany(insert_query, batch_data)
                                batch_data.clear()
                            except Exception as e:
                                logging.error(f"Error inserting batch: {e}")
                                await connection.execute('ROLLBACK')
                    if batch_data:
                        try:
                            await connection.executemany(insert_query, batch_data)
                        except Exception as e:
                            logging.error(f"Error inserting final batch: {e}")
                            await connection.execute('ROLLBACK')
        except Exception as e:
            logging.error(e)

    async def batch_upsert_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        unique_columns: Union[str, List[str]],
        batch_size: int = 250
    ) -> None:
        """
        Batch upsert a DataFrame into a table (update on conflict for non-unique columns).
        """
        try:
            async with lock:
                if not await self.table_exists(table_name):
                    await self.create_table(df, table_name, unique_columns)
                df = df.copy()
                eastern = ZoneInfo("America/New_York")
                df['insertion_timestamp'] = [datetime.now(tz=eastern).replace(tzinfo=None) for _ in range(len(df))]
                records = df.to_dict(orient='records')
                if isinstance(unique_columns, str):
                    unique_columns = [uc.strip() for uc in unique_columns.split(",")]
                async with self.pool.acquire() as connection:
                    column_types = await connection.fetch(
                        f"""
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_name = '{table_name}'
                        """
                    )
                    type_mapping = {col['column_name']: col['data_type'] for col in column_types}
                    insert_query = f"""
                    INSERT INTO {table_name} (
                        {', '.join(f'"{col}"' for col in df.columns)}
                    )
                    VALUES (
                        {', '.join(f'${i}' for i in range(1, len(df.columns) + 1))}
                    )
                    ON CONFLICT ({', '.join(f'"{col}"' for col in unique_columns)})
                    DO UPDATE SET {', '.join(f'"{col}" = EXCLUDED."{col}"'
                    for col in df.columns if col not in unique_columns)}
                    """
                    batch_data = []
                    for record in records:
                        new_record = []
                        for col, val in record.items():
                            pg_type = type_mapping.get(col)
                            if pd.isna(val):
                                new_record.append(None)
                            elif pg_type in ['timestamp', 'timestamp without time zone', 'timestamp with time zone']:
                                if isinstance(val, datetime):
                                    new_record.append(val.replace(tzinfo=None))
                                elif isinstance(val, np.datetime64):
                                    dt_val = pd.Timestamp(val).to_pydatetime().replace(tzinfo=None)
                                    new_record.append(dt_val)
                                else:
                                    new_record.append(val)
                            elif pg_type in ['double precision', 'real', 'float']:
                                new_record.append(float(val) if not isinstance(val, str) else val)
                            elif isinstance(val, np.int64):
                                new_record.append(int(val))
                            elif pg_type == 'integer' and not isinstance(val, int):
                                new_record.append(int(val))
                            else:
                                new_record.append(val)
                        batch_data.append(tuple(new_record))
                        if len(batch_data) == batch_size:
                            try:
                                await connection.executemany(insert_query, batch_data)
                                batch_data.clear()
                            except Exception as e:
                                logging.error(f"Error during batch upsert: {e}")
                                await connection.execute('ROLLBACK')
                    if batch_data:
                        try:
                            await connection.executemany(insert_query, batch_data)
                        except Exception as e:
                            logging.error(f"Error during final batch upsert: {e}")
                            await connection.execute('ROLLBACK')
        except Exception as e:
            logging.error(f"An error occurred: {e}")

    async def save_to_history(self, df: pd.DataFrame, main_table_name: str, history_table_name: str) -> None:
        """
        Save current data to a history table for archival purposes.
        """
        try:
            if not await self.table_exists(history_table_name):
                await self.create_table(df, history_table_name, None)
            df['archived_at'] = datetime.now()
            await self.batch_insert_dataframe(df, history_table_name, None)
        except Exception as e:
            logging.error(e)

    # ------------------ Example Data Retrieval and Utility Methods ------------------ #
    async def fetch_records(self) -> List[asyncpg.Record]:
        """
        Fetch all records from the 'opts' table.
        """
        await self.connect()
        select_sql = "SELECT * FROM opts"
        async with self.pool.acquire() as conn:
            records = await conn.fetch(select_sql)
            return records

    async def fetch_endpoint(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Fetch data from a Polygon API endpoint using additional parameters.
        Automatically paginates if a 'next_url' is present.
        """
        filtered_params = {k: v for k, v in (params or {}).items() if v is not None}
        if filtered_params:
            query_string = urlencode(filtered_params)
            delimiter = '&' if '?' in endpoint else '?'
            endpoint = f"{endpoint}{delimiter}{query_string}&apiKey={self.api_key}"
        else:
            delimiter = '&' if '?' in endpoint else '?'
            endpoint = f"{endpoint}{delimiter}apiKey={self.api_key}"
        data = await self.fetch_page(endpoint)
        if 'next_url' in data:
            return await self.paginate_concurrent(endpoint, as_dataframe=True, concurrency=40)
        return data

    async def update_options(self, ticker: str) -> None:
        """
        Fetch options for a ticker and insert them into the 'opts' table.
        """
        all_options = await self.find_symbols(ticker)
        df = all_options.df
        await self.batch_insert_dataframe(df, table_name='opts', unique_columns='option_symbol')

    # ------------------ Filtering Methods ------------------ #
    async def filter_options(self, select_columns: Optional[str] = None, **kwargs) -> List[asyncpg.Record]:
        """
        Filter the 'opts' table based on provided constraints.
        Returns the filtered records.
        """
        if select_columns is None:
            query = "SELECT * FROM opts WHERE oi > 0"
        else:
            query = f"SELECT ticker, strike, cp, expiry, {select_columns} FROM opts WHERE oi > 0"
        column_types = {
            'ticker': ('ticker', 'string'),
            'strike': ('strike', 'float'),
            'strike_min': ('strike', 'float'),
            'strike_max': ('strike', 'float'),
            'expiry': ('expiry', 'date'),
            'expiry_min': ('expiry', 'date'),
            'expiry_max': ('expiry', 'date'),
            'open': ('open', 'float'),
            'open_min': ('open', 'float'),
            'open_max': ('open', 'float'),
            'high': ('high', 'float'),
            'high_min': ('high', 'float'),
            'high_max': ('high', 'float'),
            'low': ('low', 'float'),
            'low_min': ('low', 'float'),
            'low_max': ('low', 'float'),
            'oi': ('oi', 'float'),
            'oi_min': ('oi', 'float'),
            'oi_max': ('oi', 'float'),
            'vol': ('vol', 'float'),
            'vol_min': ('vol', 'float'),
            'vol_max': ('vol', 'float'),
            'delta': ('delta', 'float'),
            'delta_min': ('delta', 'float'),
            'delta_max': ('delta', 'float'),
            'vega': ('vega', 'float'),
            'vega_min': ('vega', 'float'),
            'vega_max': ('vega', 'float'),
            'iv': ('iv', 'float'),
            'iv_min': ('iv', 'float'),
            'iv_max': ('iv', 'float'),
            'dte': ('dte', 'string'),
            'dte_min': ('dte', 'string'),
            'dte_max': ('dte', 'string'),
            'gamma': ('gamma', 'float'),
            'gamma_min': ('gamma', 'float'),
            'gamma_max': ('gamma', 'float'),
            'theta': ('theta', 'float'),
            'theta_min': ('theta', 'float'),
            'theta_max': ('theta', 'float'),
            'sensitivity': ('sensitivity', 'float'),
            'sensitivity_max': ('sensitivity', 'float'),
            'sensitivity_min': ('sensitivity', 'float'),
            'bid': ('bid', 'float'),
            'bid_min': ('bid', 'float'),
            'bid_max': ('bid', 'float'),
            'ask': ('ask', 'float'),
            'ask_min': ('ask', 'float'),
            'ask_max': ('ask', 'float'),
            'close': ('close', 'float'),
            'close_min': ('close', 'float'),
            'close_max': ('close', 'float'),
            'cp': ('cp', 'string'),
            'time_value': ('time_value', 'float'),
            'time_value_min': ('time_value', 'float'),
            'time_value_max': ('time_value', 'float'),
            'moneyness': ('moneyness', 'string'),
            'exercise_style': ('exercise_style', 'string'),
            'option_symbol': ('option_symbol', 'string'),
            'theta_decay_rate': ('theta_decay_rate', 'float'),
            'theta_decay_rate_min': ('theta_decay_rate', 'float'),
            'theta_decay_rate_max': ('theta_decay_rate', 'float'),
            'delta_theta_ratio': ('delta_theta_ratio', 'float'),
            'delta_theta_ratio_min': ('delta_theta_ratio', 'float'),
            'delta_theta_ratio_max': ('delta_theta_ratio', 'float'),
            'gamma_risk': ('gamma_risk', 'float'),
            'gamma_risk_min': ('gamma_risk', 'float'),
            'gamma_risk_max': ('gamma_risk', 'float'),
            'vega_impact': ('vega_impact', 'float'),
            'vega_impact_min': ('vega_impact', 'float'),
            'vega_impact_max': ('vega_impact', 'float'),
            'intrinsic_value_min': ('intrinsic_value', 'float'),
            'intrinsic_value_max': ('intrinsic_value', 'float'),
            'intrinsic_value': ('intrinsic_value', 'float'),
            'extrinsic_value': ('extrinsic_value', 'float'),
            'extrinsic_value_min': ('extrinsic_value', 'float'),
            'extrinsic_value_max': ('extrinsic_value', 'float'),
            'leverage_ratio': ('leverage_ratio', 'float'),
            'leverage_ratio_min': ('leverage_ratio', 'float'),
            'leverage_ratio_max': ('leverage_ratio', 'float'),
            'vwap': ('vwap', 'float'),
            'vwap_min': ('vwap', 'float'),
            'vwap_max': ('vwap', 'float'),
            'price': ('price', 'float'),
            'price_min': ('price', 'float'),
            'price_max': ('price', 'float'),
            'trade_size': ('trade_size', 'float'),
            'trade_size_min': ('trade_size', 'float'),
            'trade_size_max': ('trade_size', 'float'),
            'spread': ('spread', 'float'),
            'spread_min': ('spread', 'float'),
            'spread_max': ('spread', 'float'),
            'spread_pct': ('spread_pct', 'float'),
            'spread_pct_min': ('spread_pct', 'float'),
            'spread_pct_max': ('spread_pct', 'float'),
            'bid_size': ('bid_size', 'float'),
            'bid_size_min': ('bid_size', 'float'),
            'bid_size_max': ('bid_size', 'float'),
            'ask_size': ('ask_size', 'float'),
            'ask_size_min': ('ask_size', 'float'),
            'ask_size_max': ('ask_size', 'float'),
            'mid': ('mid', 'float'),
            'mid_min': ('mid', 'float'),
            'mid_max': ('mid', 'float'),
            'change_to_breakeven': ('change_to_breakeven', 'float'),
            'change_to_breakeven_min': ('change_to_breakeven', 'float'),
            'change_to_breakeven_max': ('change_to_breakeven', 'float'),
            'underlying_price': ('underlying_price', 'float'),
            'underlying_price_min': ('underlying_price', 'float'),
            'underlying_price_max': ('underlying_price', 'float'),
            'return_on_risk': ('return_on_risk', 'float'),
            'return_on_risk_min': ('return_on_risk', 'float'),
            'return_on_risk_max': ('return_on_risk', 'float'),
            'velocity': ('velocity', 'float'),
            'velocity_min': ('velocity', 'float'),
            'velocity_max': ('velocity', 'float'),
            'greeks_balance': ('greeks_balance', 'float'),
            'greeks_balance_min': ('greeks_balance', 'float'),
            'greeks_balance_max': ('greeks_balance', 'float'),
            'opp': ('opp', 'float'),
            'opp_min': ('opp', 'float'),
            'opp_max': ('opp', 'float'),
            'liquidity_score': ('liquidity_score', 'float'),
            'liquidity_score_min': ('liquidity_score', 'float'),
            'liquidity_score_max': ('liquidity_score', 'float')
        }
        for key, value in kwargs.items():
            if key in column_types and value is not None:
                column, col_type = column_types[key]
                sanitized_value = self.sanitize_value(value, col_type)
                if 'min' in key:
                    query += f" AND {column} >= {sanitized_value}"
                elif 'max' in key:
                    query += f" AND {column} <= {sanitized_value}"
                else:
                    query += f" AND {column} = {sanitized_value}"
        try:
            async with self.pool.acquire() as conn:
                return await conn.fetch(query)
        except Exception as e:
            logging.error(f"Error during query: {e}")
            return []

    # ------------------ Higher-Level Methods for Option Data ------------------ #
    async def get_option_chain_all(
        self,
        underlying_asset: str,
        strike_price: Optional[float] = None,
        strike_price_lte: Optional[float] = None,
        strike_price_gte: Optional[float] = None,
        expiration_date: Optional[str] = None,
        expiration_date_gte: Optional[str] = None,
        expiration_date_lte: Optional[str] = None,
        contract_type: Optional[str] = None,
        order: Optional[str] = None,
        limit: int = 250,
        sort: Optional[str] = None,
        insert: bool = False
    ) -> Optional[UniversalOptionSnapshot]:
        """
        Retrieve all option contracts for a given underlying asset with optional filters.
        Optionally, insert the data into the 'all_options' table.
        """
        try:
            if not underlying_asset:
                raise ValueError("Underlying asset ticker symbol must be provided.")
            if underlying_asset.startswith("I:"):
                underlying_asset = underlying_asset.replace("I:", "")
            params = {
                'strike_price': strike_price,
                'strike_price.lte': strike_price_lte,
                'strike_price.gte': strike_price_gte,
                'expiration_date': expiration_date,
                'expiration_date.gte': expiration_date_gte,
                'expiration_date.lte': expiration_date_lte,
                'contract_type': contract_type,
                'order': order,
                'limit': limit,
                'sort': sort
            }
            params = {k: v for k, v in params.items() if v is not None}
            endpoint = f"https://api.polygon.io/v3/snapshot/options/{underlying_asset}"
            if params:
                query_string = '&'.join(f"{key}={value}" for key, value in params.items())
                endpoint = f"{endpoint}?{query_string}&apiKey={self.api_key}"
            else:
                endpoint = f"{endpoint}?apiKey={self.api_key}"
            response_data = await self.paginate_concurrent(endpoint)
            option_data = UniversalOptionSnapshot(response_data)
            if insert:
                await self.connect()
                await self.batch_insert_dataframe(option_data.df, table_name='all_options', unique_columns='option_symbol')
            return option_data
        except ValueError as ve:
            logging.error(f"ValueError occurred: {ve}")
        except Exception as e:
            logging.error(f"Error in get_option_chain_all: {e}")
            return None

    async def find_symbols(
        self,
        underlying_asset: str,
        strike_price: Optional[float] = None,
        strike_price_lte: Optional[float] = None,
        strike_price_gte: Optional[float] = None,
        expiration_date: Optional[str] = None,
        expiration_date_gte: Optional[str] = None,
        expiration_date_lite: Optional[str] = None,
        contract_type: Optional[str] = None,
        order: Optional[str] = None,
        limit: int = 250,
        sort: Optional[str] = None
    ) -> UniversalOptionSnapshot:
        """
        Retrieve all option contracts for an underlying asset and return a UniversalOptionSnapshot.
        """
        params = {
            'strike_price': strike_price,
            'strike_price.lte': strike_price_lte,
            'strike_price.gte': strike_price_gte,
            'expiration_date': expiration_date,
            'expiration_date.gte': expiration_date_gte,
            'expiration_date.lte': expiration_date_lite,
            'contract_type': contract_type,
            'order': order,
            'limit': limit,
            'sort': sort
        }
        params = {k: v for k, v in params.items() if v is not None}
        endpoint = f"https://api.polygon.io/v3/snapshot/options/{underlying_asset}"
        if params:
            query_string = '&'.join(f"{k}={v}" for k, v in params.items())
            endpoint += f"?{query_string}&apiKey={self.api_key}"
        else:
            endpoint += f"?apiKey={self.api_key}"
        response_data = await self.paginate_concurrent(endpoint)
        return UniversalOptionSnapshot(response_data)

    async def get_universal_snapshot(self, ticker: str) -> Optional[UniversalOptionSnapshot]:
        """
        Fetch the universal snapshot for a given ticker.
        """
        url = f"https://api.polygon.io/v3/snapshot?ticker.any_of={ticker}&limit=250&apiKey={self.api_key}"
        logging.info(f"Fetching universal snapshot: {url}")
        async with httpx.AsyncClient() as client:
            resp = await client.get(url)
            data = resp.json()
            results = data.get('results')
            if results is not None:
                return UniversalOptionSnapshot(results)
            return None

    async def working_universal(self, symbols_list: List[str]) -> Optional[WorkingUniversal]:
        """
        Fetch universal snapshots for a large list of symbols in chunks.
        """
        async def fetch_chunk(chunk: List[str]) -> Dict[str, Any]:
            symbols_str = ",".join(chunk)
            url = f"https://api.polygon.io/v3/snapshot?ticker.any_of={symbols_str}&limit=250&apiKey={self.api_key}"
            async with httpx.AsyncClient() as client:
                resp = await client.get(url)
                resp.raise_for_status()
                return resp.json()
        chunk_size = 249
        chunks = list(chunked(symbols_list, chunk_size))
        results = []
        for chunk in chunks:
            data = await fetch_chunk(chunk)
            if "results" in data:
                results.extend(data["results"])
        return WorkingUniversal(results) if results else None

    # ------------------ Price & Trade Methods ------------------ #
    async def get_price(self, ticker: str) -> Optional[float]:
        """
        Fetch the close price for a ticker using the universal snapshot endpoint.
        """
        try:
            if ticker in ['SPX', 'NDX', 'XSP', 'RUT', 'VIX']:
                ticker = f"I:{ticker}"
            url = f"https://api.polygon.io/v3/snapshot?ticker.any_of={ticker}&limit=1&apiKey={self.api_key}"
            async with httpx.AsyncClient() as client:
                r = await client.get(url)
                if r.status_code == 200:
                    resp_data = r.json()
                    results = resp_data.get('results', [])
                    if results:
                        session_data = results[0].get('session')
                        if session_data:
                            return session_data.get('close')
            return None
        except Exception as e:
            logging.error(f"Error fetching price for {ticker}: {e}")
            return None

    async def _get_price_for_ticker(self, ticker: str, client: httpx.AsyncClient) -> Optional[float]:
        """
        Helper: Fetch the latest close price for a single ticker.
        """
        try:
            url = f"https://api.polygon.io/v3/snapshot?ticker.any_of={ticker}&limit=1&apiKey={self.api_key}"
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            results = data.get("results")
            if results:
                session = results[0].get("session")
                if session and "close" in session:
                    return session["close"]
            return None
        except Exception as e:
            logging.error(f"Error fetching price for {ticker}: {e}")
            return None

    async def get_price_for_tickers(self, tickers: List[str]) -> AsyncGenerator[Tuple[str, Optional[float]], None]:
        """
        Asynchronously fetch close prices for multiple tickers.
        Yields tuples of (ticker, price).
        """
        async with httpx.AsyncClient() as client:
            tasks = [asyncio.create_task(self._get_price_for_ticker(ticker, client)) for ticker in tickers]
            for completed in asyncio.as_completed(tasks):
                price = await completed
                # Note: The attached ticker is not tracked automatically here.
                # In production, you might want to attach the ticker with the task.
                yield tickers[0], price  # (Placeholder: adjust to correctly yield the corresponding ticker)

    async def get_symbols(self, ticker: str) -> List[str]:
        """
        Fetch current price and option chain for the next 15 days, and return a list of option symbols.
        """
        try:
            price = await self.get_price(ticker)
            logging.info(f"Price for {ticker}: {price}")
            options_df = await self.get_option_chain_all(
                underlying_asset=ticker,
                expiration_date_gte=self.today,
                expiration_date_lte=self.fifteen_days_from_now
            )
            logging.info(f"Number of options for {ticker}: {len(options_df.df)}")
            return options_df.option_symbol
        except Exception as e:
            logging.error(e)
            return []

    async def get_trades(self, symbol: str, client: httpx.AsyncClient) -> Optional[pd.DataFrame]:
        """
        Fetch trade data for an option symbol and store it in the 'option_trades' table.
        """
        try:
            url = f"https://api.polygon.io/v3/trades/{symbol}?limit=50000&apiKey={self.api_key}"
            response = await client.get(url)
            if response.status_code == 200:
                data = response.json()
                results = data.get('results', [])
                if not results:
                    return None

                pattern = r'^O:(?P<ticker>[A-Z]+)(?P<expiry>\d{6})(?P<call_put>[CP])(?P<strike>\d{8})$'
                match_obj = re.match(pattern, symbol)
                if match_obj:
                    ticker = match_obj.group('ticker')
                    expiry_str = match_obj.group('expiry')
                    call_put = match_obj.group('call_put')
                    strike_str = match_obj.group('strike')
                    expiry = datetime.strptime(expiry_str, '%y%m%d').date()
                    strike = int(strike_str) / 1000.0
                else:
                    logging.warning(f"Symbol {symbol} didn't match the pattern.")
                    ticker = ''
                    expiry = None
                    call_put = ''
                    strike = 0.0

                conditions_list = [
                    ', '.join(option_conditions.get(cond, 'Unknown Condition') for cond in trade.get('conditions', []))
                    for trade in results
                ]
                exchanges = [
                    OPTIONS_EXCHANGES.get(trade.get('exchange'), 'Unknown Exchange')
                    for trade in results
                ]
                ids = [trade.get('id') for trade in results]
                prices = [trade.get('price') for trade in results]
                sequence_numbers = [trade.get('sequence_number') for trade in results]
                sip_timestamps = [to_datetime_eastern(trade.get('sip_timestamp')) for trade in results]
                sizes = [trade.get('size') for trade in results]
                dollar_costs = [p * s for p, s in zip(prices, sizes)]

                data_dict = {
                    'ticker': [ticker] * len(results),
                    'strike': [strike] * len(results),
                    'call_put': [call_put] * len(results),
                    'expiry': [expiry] * len(results),
                    'option_symbol': [symbol] * len(results),
                    'conditions': conditions_list,
                    'price': prices,
                    'size': sizes,
                    'dollar_cost': dollar_costs,
                    'timestamp': sip_timestamps,
                    'sequence_number': sequence_numbers,
                    'id': ids,
                    'exchange': exchanges
                }
                df = pd.DataFrame(data_dict)
                df['average_price'] = df['price'].mean()
                df['average_size'] = df['size'].mean()
                df['highest_price'] = df['price'].max()
                df['lowest_price'] = df['price'].min()
                df['highest_size'] = df['size'].max()
                await self.batch_upsert_dataframe(
                    df,
                    table_name='option_trades',
                    unique_columns=['option_symbol', 'timestamp', 'sequence_number', 'exchange', 'conditions', 'size', 'price', 'dollar_cost']
                )
                return df
            else:
                logging.error(f"Failed retrieving data for {symbol}. Status code: {response.status_code}")
                return None
        except Exception as e:
            logging.error(e)
            return None
        


    async def get_trades_for_symbols(self, symbols: List[str]) -> pd.DataFrame:
        """
        Fetch trades data for multiple option symbols concurrently and return a combined DataFrame.
        """
        async with httpx.AsyncClient() as client:
            tasks = [self.get_trades(symbol, client) for symbol in symbols]
            results = await asyncio.gather(*tasks)
            dataframes = [df for df in results if df is not None]
            if dataframes:
                return pd.concat(dataframes, ignore_index=True)
            return pd.DataFrame()

    async def fetch_option_aggs(
        self,
        option_symbol: str,
        start_date: str = "2021-01-01",
        timespan: str = 'day'
    ) -> pd.DataFrame:
        """
        Fetch historical aggregated data for an option symbol.
        """
        end_date = datetime.today().strftime('%Y-%m-%d')
        url_template = ("https://api.polygon.io/v2/aggs/ticker/{}/range/1/{}/{}/{}?"
                        "adjusted=true&sort=asc&limit=50000&apiKey={}")
        url = url_template.format(option_symbol, timespan, start_date, end_date, self.api_key)
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            if response.status_code == 200:
                data = response.json()
                if 'results' in data:
                    df = pd.DataFrame(data['results'])
                    df['date'] = pd.to_datetime(df['t'], unit='ms').dt.date
                    df.rename(columns={'o': 'open', 'c': 'close', 'h': 'high', 'l': 'low', 'v': 'volume'}, inplace=True)
                    return df[['date', 'open', 'close', 'high', 'low', 'volume']]
            logging.error(f"No data available for {option_symbol}. Code: {response.status_code}")
            return pd.DataFrame()

    async def find_lowest_highest_price_df(
        self,
        ticker: str,
        strike: int,
        expiry: str,
        call_put: str
    ) -> pd.DataFrame:
        """
        Find the option symbol based on ticker, strike, expiry, and call/put, then
        fetch historical data to determine the lowest and highest prices.
        """
        await self.connect()
        query = (f"SELECT option_symbol FROM master_all_two WHERE ticker = '{ticker}' AND strike = {strike} "
                 f"AND call_put = '{call_put}' AND expiry = '{expiry}'")
        results = await self.fetch(query)
        df = pd.DataFrame(results, columns=['option_symbol'])
        if df.empty:
            logging.warning("No matching option_symbol found.")
            return pd.DataFrame({"type": [], "date": [], "price": []})
        option_symbol = df['option_symbol'].to_list()[0]
        df_hist = await self.fetch_option_aggs(option_symbol)
        if df_hist.empty:
            return pd.DataFrame({
                "type": ["lowest", "highest"],
                "date": ["N/A", "N/A"],
                "price": [float('inf'), float('-inf')]
            })
        min_row = df_hist.loc[df_hist['low'].idxmin()]
        lowest_price = min_row['low']
        lowest_date = min_row['date'].strftime('%Y-%m-%d')
        max_row = df_hist.loc[df_hist['high'].idxmax()]
        highest_price = max_row['high']
        highest_date = max_row['date'].strftime('%Y-%m-%d')
        return pd.DataFrame({
            "type": ["lowest", "highest"],
            "date": [lowest_date, highest_date],
            "price": [lowest_price, highest_price]
        })

    async def rsi(
        self,
        ticker: str,
        timespan: str,
        limit: str = '1000',
        window: int = 14,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None
    ) -> Optional[RSI]:
        """
        Fetch RSI data for a ticker from the Polygon indicators endpoint.
        """
        if date_from is None:
            date_from = self.eight_days_ago
        if date_to is None:
            date_to = self.today
        endpoint = (f"https://api.polygon.io/v1/indicators/rsi/{ticker}?"
                    f"timespan={timespan}&timestamp.gte={date_from}&timestamp.lte={date_to}&"
                    f"limit={limit}&window={window}&apiKey={self.api_key}")
        session = await self.get_http_session()
        try:
            async with session.get(endpoint) as resp:
                datas = await resp.json()
                if datas is not None:
                    return RSI(datas, ticker)
        except (aiohttp.ClientConnectorError, aiohttp.ClientOSError, aiohttp.ContentTypeError) as e:
            logging.error(f"RSI error for {ticker} - {e}")
            return None

    async def rsi_snapshot(self, ticker: str) -> pd.DataFrame:
        """
        Fetch RSI values for multiple timespans and return a DataFrame of the results.
        """
        try:
            components = get_human_readable_string(ticker)
            symbol = components.get('underlying_symbol')
            strike = components.get('strike_price')
            expiry = components.get('expiry_date')
            call_put = str(components.get('call_put')).lower()
            timespans = ['minute', 'hour', 'day', 'week', 'month']
            all_data_dicts = []
            for ts in timespans:
                rsi_data = await self.rsi(ticker, ts, limit='1')
                if rsi_data and rsi_data.rsi_value and len(rsi_data.rsi_value) > 0:
                    rsi_val = rsi_data.rsi_value[0]
                else:
                    rsi_val = 0
                all_data_dicts.append({
                    'timespan': ts,
                    'option_symbol': ticker,
                    'ticker': symbol,
                    'strike': strike,
                    'expiry': expiry,
                    'call_put': call_put,
                    'rsi': rsi_val
                })
            return pd.DataFrame(all_data_dicts)
        except Exception as e:
            logging.error(e)
            return pd.DataFrame()

    async def option_aggregates(
        self,
        ticker: str,
        timespan: str = 'second',
        date_from: str = '2019-09-17',
        date_to: Optional[str] = None,
        limit: int = 50000,
        as_dataframe: bool = False
    ) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
        """
        Fetch historical aggregates for a ticker or option symbol.
        """
        if date_to is None:
            date_to = self.today
        url = (f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/{timespan}/"
               f"{date_from}/{date_to}?adjusted=true&sort=desc&limit={limit}&apiKey={self.api_key}")
        results = await self.paginate_concurrent(url, as_dataframe=False, concurrency=50)
        if as_dataframe:
            return pd.DataFrame(results)
        return results

    # ------------------ Additional Option Data Methods ------------------ #
    async def gpt_filter(self, **kwargs) -> List[str]:
        """
        Filter the 'opts' table based on provided constraints and return table chunks.
        """
        await self.connect()
        records = await self.filter_options(**kwargs)
        df = pd.DataFrame(records)
        if df.empty:
            return ["No records found."]
        table = tabulate(df, headers=df.columns, tablefmt='fancy_grid', showindex=False)
        chunks = chunk_string(table, 4000)
        return chunks

    def binomial_american_option(self, S: float, K: float, T: float, r: float, sigma: float, N: int, option_type: str = 'put') -> float:
        """
        Price an American option using the binomial model.
        """
        dt = T / N
        u = np.exp(sigma * np.sqrt(dt))
        d = 1 / u
        p = (np.exp(r * dt) - d) / (u - d)
        ST = np.zeros((N + 1, N + 1))
        ST[0, 0] = S
        for i in range(1, N + 1):
            ST[i, 0] = ST[i - 1, 0] * u
            for j in range(1, i + 1):
                ST[i, j] = ST[i - 1, j - 1] * d
        option = np.zeros((N + 1, N + 1))
        for j in range(N + 1):
            if option_type == 'call':
                option[N, j] = max(0, ST[N, j] - K)
            else:
                option[N, j] = max(0, K - ST[N, j])
        for i in range(N - 1, -1, -1):
            for j in range(i + 1):
                exercise_value = max(ST[i, j] - K, 0) if option_type == 'call' else max(K - ST[i, j], 0)
                hold_value = np.exp(-r * dt) * (p * option[i + 1, j] + (1 - p) * option[i + 1, j + 1])
                option[i, j] = max(exercise_value, hold_value)
        return option[0, 0]

    async def get_theoretical_price(self, ticker: str) -> List[Dict[str, Any]]:
        """
        Calculate theoretical option prices using a binomial model.
        This is a placeholder that uses dummy data for demonstration.
        """
        current_price = 150.0  # Placeholder; replace with real fetch
        all_options_data = await self.get_option_chain_all(underlying_asset=ticker)
        risk_free_rate = 0.0565
        N = 100
        theoretical_values = []
        for idx, row in all_options_data.df.iterrows():
            bid = row.get('bid')
            ask = row.get('ask')
            iv = row.get('implied_volatility')
            strike = row.get('strike')
            expiry = row.get('expiry')
            option_type = row.get('contract_type')
            dte = row.get('days_to_expiry', 30)
            T = float(dte) / 365.0
            if iv is None:
                continue
            sigma = iv
            theoretical_price = self.binomial_american_option(S=current_price, K=strike, T=T, r=risk_free_rate, sigma=sigma, N=N, option_type=option_type)
            theoretical_values.append({
                'ticker': ticker,
                'current_price': current_price,
                'iv': iv,
                'strike': strike,
                'expiry': expiry,
                'bid': bid,
                'theoretical_price': theoretical_price,
                'ask': ask,
                'type': option_type
            })
        return theoretical_values

    # ------------------ Accessor Methods ------------------ #
    async def get_table_columns(self, table: str) -> List[str]:
        """
        Fetch column names for a specified table.
        """
        try:
            conn = await asyncpg.connect(self.connection_string)
            query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = $1
            ORDER BY ordinal_position;
            """
            rows = await conn.fetch(query, table)
            await conn.close()
            return [row['column_name'] for row in rows]
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            return []

