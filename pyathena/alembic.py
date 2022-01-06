# -*- coding: utf-8; -*-
from alembic.ddl.impl import DefaultImpl
from alembic.ddl.base import AddColumn
from alembic.ddl.base import alter_table
from sqlalchemy.ext.compiler import compiles

from sqlalchemy.sql.sqltypes import INTEGER, Integer
from sqlalchemy.schema import CreateColumn


_DIALECT_NAME = 'awsathena'


class AWSAthenaImpl(DefaultImpl):
    __dialect__ = _DIALECT_NAME



@compiles(AddColumn, _DIALECT_NAME)
def visit_add_column(element, compiler, **kw):
    return "%s %s" % (alter_table(compiler, element.table_name, element.schema),
                      add_columns(compiler, element.column, **kw),
                      )


def add_columns(compiler, column, **kw):
    return "ADD COLUMNS (%s)" % compiler.get_column_specification(column, **kw)


@compiles(Integer, _DIALECT_NAME)
@compiles(INTEGER, _DIALECT_NAME)
def visit_INTEGER(element, ddlcompiler, **kw):
    return 'int'


# vim: et:sw=4:syntax=python:ts=4:
