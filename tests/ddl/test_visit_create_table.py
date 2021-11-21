# -*- coding: utf-8; -*-
import re

from sqlalchemy import Column, Integer, MetaData, Table
from sqlalchemy.sql.ddl import CreateTable


def test_create_table_location(dialect, table_overrides, value_getter):
    # Given
    table = Table('table_name',
                  MetaData(),
                  Column('column_name', Integer),
                  **table_overrides)
    s3_prefix = value_getter('s3_prefix')
    s3_dir = value_getter('s3_dir')

    # When
    location = re.findall("(?:LOCATION ')([^']+)(?:')",
                          str(CreateTable(table).compile(dialect=dialect)))[0]

    # Then
    assert location == f"{s3_dir}{'/' if s3_prefix else ''}{s3_prefix}/{table.name}"


# vim: et:sw=4:syntax=python:ts=4:
