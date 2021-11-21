# -*- coding: utf-8 -*-
import contextlib
import os
from typing import Any, Callable, Dict
import uuid

import pytest

from pyathena import connect
from pyathena.sqlalchemy_athena import AthenaDialect
from tests import BASE_PATH, ENV, S3_PREFIX, SCHEMA
from tests.util import read_query


@pytest.fixture(scope="session", autouse=True)
def _setup_session(request):
    request.addfinalizer(_teardown_session)
    with contextlib.closing(connect()) as conn:
        with conn.cursor() as cursor:
            _create_database(cursor)
            _create_table(cursor)


def _teardown_session():
    with contextlib.closing(connect()) as conn:
        with conn.cursor() as cursor:
            _drop_database(cursor)


def _create_database(cursor):
    for q in read_query(os.path.join(BASE_PATH, "sql", "create_database.sql")):
        cursor.execute(q.format(schema=SCHEMA))


def _drop_database(cursor):
    for q in read_query(os.path.join(BASE_PATH, "sql", "drop_database.sql")):
        cursor.execute(q.format(schema=SCHEMA))


def _create_table(cursor):
    location_one_row = "{0}{1}/{2}/".format(ENV.s3_staging_dir, S3_PREFIX, "one_row")
    location_many_rows = "{0}{1}/{2}/".format(
        ENV.s3_staging_dir, S3_PREFIX, "many_rows"
    )
    location_one_row_complex = "{0}{1}/{2}/".format(
        ENV.s3_staging_dir, S3_PREFIX, "one_row_complex"
    )
    location_partition_table = "{0}{1}/{2}/".format(
        ENV.s3_staging_dir, S3_PREFIX, "partition_table"
    )
    location_integer_na_values = "{0}{1}/{2}/".format(
        ENV.s3_staging_dir, S3_PREFIX, "integer_na_values"
    )
    location_boolean_na_values = "{0}{1}/{2}/".format(
        ENV.s3_staging_dir, S3_PREFIX, "boolean_na_values"
    )
    location_execute_many = "{0}{1}/{2}/".format(
        ENV.s3_staging_dir,
        S3_PREFIX,
        "execute_many_{0}".format(str(uuid.uuid4()).replace("-", "")),
    )
    location_execute_many_pandas = "{0}{1}/{2}/".format(
        ENV.s3_staging_dir,
        S3_PREFIX,
        "execute_many_pandas_{0}".format(str(uuid.uuid4()).replace("-", "")),
    )
    for q in read_query(os.path.join(BASE_PATH, "sql", "create_table.sql")):
        cursor.execute(
            q.format(
                schema=SCHEMA,
                location_one_row=location_one_row,
                location_many_rows=location_many_rows,
                location_one_row_complex=location_one_row_complex,
                location_partition_table=location_partition_table,
                location_integer_na_values=location_integer_na_values,
                location_boolean_na_values=location_boolean_na_values,
                location_execute_many=location_execute_many,
                location_execute_many_pandas=location_execute_many_pandas,
            )
        )


@pytest.fixture(params=(
    pytest.param({'s3_dir': 's3://some-bucket'}, id='s3_dir overrides'),
    pytest.param({'s3_dir': 's3://some-bucket/'}, id='s3_dir tailing slash'),
    pytest.param({'s3_dir': 's3://some-bucket/',
                  's3_prefix': '/prefix',
                  }, id='dialect overrides: s3_dir tailing slash, s3_prefix left slash'),
    pytest.param({'s3_dir': 's3://some-bucket/',
                  's3_prefix': 'prefix/',
                  }, id='dialect overrides: s3_dir tailing slash, s3_prefix right slash'),
    pytest.param({'s3_dir': 's3://some-bucket/',
                  's3_prefix': '/prefix/',
                  }, id='dialect overrides: s3_dir tailing slash, s3_prefix both slash'),
))
def dialect_overrides(request) -> Dict[str, str]:
    overrides: Dict[str, Any] = request.param
    return overrides


@pytest.fixture()
def dialect(dialect_overrides: Dict[str, str]) -> AthenaDialect:
    return AthenaDialect(**dialect_overrides)


@pytest.fixture(params=(
    pytest.param({}, id='no table overrides'),
    pytest.param({'s3_dir': 's3://some-table-override-bucket'}, id='table: s3_dir'),
    pytest.param({'schema': 'schema_name'}, id='table: schema'),
    pytest.param({'s3_prefix': '/prefix/'}, id='table: s3_prefix'),
    pytest.param({'s3_prefix': '/prefix/',
                  'schema': 'schema_name',
                  }, id='table: s3_prefix, schema'),
))
def table_overrides(request) -> Dict[str, str]:
    return {f'awsathena_{k}' if not k.startswith('awsathena_') and k not in ('schema') else k: v
            for k, v in request.param.items()}


@pytest.fixture
def value_getter(dialect, dialect_overrides, table_overrides) -> Callable[[str], str]:
    def _value_getter(key: str) -> str:
        if(key == 's3_prefix'
           and 'schema' in table_overrides
           and f'awsathena_{key}' not in table_overrides
           and key not in dialect_overrides):
            value = table_overrides['schema']
        elif f'awsathena_{key}' in table_overrides:
            value = table_overrides[f'awsathena_{key}']
        elif key in dialect_overrides:
            value = dialect_overrides[key]
        else:
            value = [v[1][key]
                     for v in AthenaDialect.construct_arguments if v[0] is Table][0]
        if key == 's3_prefix':
            return value.strip('/')
        else:
            return value.rstrip('/')
    return _value_getter

