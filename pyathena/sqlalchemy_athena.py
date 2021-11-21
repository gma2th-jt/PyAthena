# -*- coding: utf-8 -*-
import copy
import math
import numbers
import re
from distutils.util import strtobool
from typing import Any, Dict, List, Mapping, Tuple

import tenacity
from sqlalchemy import exc, schema, util
from sqlalchemy.engine import Engine, reflection
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.exc import NoSuchTableError, OperationalError
from sqlalchemy.sql.base import DialectKWArgs
from sqlalchemy.sql.compiler import (
    DDLCompiler,
    GenericTypeCompiler,
    IdentifierPreparer,
    SQLCompiler,
)
from sqlalchemy.sql.sqltypes import (
    BIGINT,
    BINARY,
    BOOLEAN,
    DATE,
    DECIMAL,
    FLOAT,
    INTEGER,
    NULLTYPE,
    STRINGTYPE,
    TIMESTAMP,
)
from tenacity import retry_if_exception, stop_after_attempt, wait_exponential

import pyathena

_DEFAULT = object()  # Use to mark a dialect option as being left unspecified
BLANKS = re.compile(r"\s\s*")
# TODO: is there a better place for those ?
LIMIT_COMMENT_MEMBER = 255
# TODO: experimented with this. Not sure there is a limit. Still happy with
# more than 128kB. Maybe we should put the column comments here instead of
# truncating them in an arbitrary way.
LIMIT_COMMENT_TABLE = None


def _format_partitioned_by(properties: Dict[str, str]) -> str:
    """
    Examples:
        >>> print(_format_partitioned_by({'a': 'b'}))
        PARTITIONED BY (
            a b
        )
        >>> print(_format_partitioned_by({'a': 'b', 'c': 'd'}))
        PARTITIONED BY (
            a b,
            c d
        )
    """
    s = ",\n    ".join([f"{k} {v}" for k, v in properties.items()])
    return f"PARTITIONED BY (\n    {s}\n)"


def _format_tblproperties(properties: Dict[str, str]) -> str:
    """
    Example:
        >>> print(_format_tblproperties({'a': 'b'}))
        TBLPROPERTIES (
            'a'='b'
        )
        >>> print(_format_tblproperties({'a': 'b', 'c': 'd'}))
        TBLPROPERTIES (
            'a'='b',
            'c'='d'
        )
    """
    s = ",\n    ".join([f"'{k}'='{v}'" for k, v in properties.items()])
    return f"TBLPROPERTIES (\n    {s}\n)"


def process_comment_literal(value, dialect, squash_blanks=False):
    """Escapes comments in DDL statements

    String literals in COMMENT are not escaped in the same way as data
    literal strings. Data literal use '' (double quotes), comments escape
    single quotes with a \\ (backslash).
    Additionally, column comments do not support the presence of new line
    characters while table comments do. Hence the ``squash_blanks`` argument.
    """
    value = value.replace("'", r"\'")
    if squash_blanks:
        value = BLANKS.sub(" ", value)  # Replace blanks with plain spaces
    if dialect.identifier_preparer._double_percents:
        value = value.replace("%", "%%")

    return "'%s'" % value


class UniversalSet(object):
    """UniversalSet

    https://github.com/dropbox/PyHive/blob/master/pyhive/common.py"""

    def __contains__(self, item):
        return True


class AthenaDMLIdentifierPreparer(IdentifierPreparer):
    """PrestoIdentifierPreparer

    https://github.com/dropbox/PyHive/blob/master/pyhive/sqlalchemy_presto.py"""

    reserved_words = UniversalSet()


class AthenaDDLIdentifierPreparer(IdentifierPreparer):
    def __init__(
        self,
        dialect,
        initial_quote="`",
        final_quote=None,
        escape_quote="`",
        quote_case_sensitive_collations=True,
        omit_schema=False,
    ):
        super(AthenaDDLIdentifierPreparer, self).__init__(
            dialect=dialect,
            initial_quote=initial_quote,
            final_quote=final_quote,
            escape_quote=escape_quote,
            quote_case_sensitive_collations=quote_case_sensitive_collations,
            omit_schema=omit_schema,
        )


class AthenaStatementCompiler(SQLCompiler):
    """PrestoCompiler

    https://github.com/dropbox/PyHive/blob/master/pyhive/sqlalchemy_presto.py"""

    def visit_char_length_func(self, fn, **kw):
        return "length{0}".format(self.function_argspec(fn, **kw))


class AthenaTypeCompiler(GenericTypeCompiler):
    def visit_FLOAT(self, type_, **kw):
        return self.visit_REAL(type_, **kw)

    def visit_REAL(self, type_, **kw):
        return "DOUBLE"

    def visit_NUMERIC(self, type_, **kw):
        return self.visit_DECIMAL(type_, **kw)

    def visit_DECIMAL(self, type_, **kw):
        if type_.precision is None:
            return "DECIMAL"
        elif type_.scale is None:
            return "DECIMAL(%(precision)s)" % {"precision": type_.precision}
        else:
            return "DECIMAL(%(precision)s, %(scale)s)" % {
                "precision": type_.precision,
                "scale": type_.scale,
            }

    def visit_INTEGER(self, type_, **kw):
        return "INTEGER"

    def visit_SMALLINT(self, type_, **kw):
        return "SMALLINT"

    def visit_BIGINT(self, type_, **kw):
        return "BIGINT"

    def visit_TIMESTAMP(self, type_, **kw):
        return "TIMESTAMP"

    def visit_DATETIME(self, type_, **kw):
        return self.visit_TIMESTAMP(type_, **kw)

    def visit_DATE(self, type_, **kw):
        return "DATE"

    def visit_TIME(self, type_, **kw):
        raise exc.CompileError("Data type `{0}` is not supported".format(type_))

    def visit_CLOB(self, type_, **kw):
        return self.visit_BINARY(type_, **kw)

    def visit_NCLOB(self, type_, **kw):
        return self.visit_BINARY(type_, **kw)

    def visit_CHAR(self, type_, **kw):
        return self._render_string_type(type_, "CHAR")

    def visit_NCHAR(self, type_, **kw):
        return self._render_string_type(type_, "CHAR")

    def visit_VARCHAR(self, type_, **kw):
        return self._render_string_type(type_, "VARCHAR")

    def visit_NVARCHAR(self, type_, **kw):
        return self._render_string_type(type_, "VARCHAR")

    def visit_TEXT(self, type_, **kw):
        return "STRING"

    def visit_BLOB(self, type_, **kw):
        return self.visit_BINARY(type_, **kw)

    def visit_BINARY(self, type_, **kw):
        return "BINARY"

    def visit_VARBINARY(self, type_, **kw):
        return self.visit_BINARY(type_, **kw)

    def visit_BOOLEAN(self, type_, **kw):
        return "BOOLEAN"

    def visit_ARRAY(self, type_, **kw):
        # TODO: Handle visit of item type
        return f"ARRAY<{self.process(type_.item_type)}>"


class AthenaDDLCompiler(DDLCompiler):
    @property
    def preparer(self):
        return self._preparer

    @preparer.setter
    def preparer(self, value):
        pass

    def __init__(
        self,
        dialect,
        statement,
        bind=None,
        schema_translate_map=None,
        compile_kwargs=util.immutabledict(),
    ):
        self._preparer = AthenaDDLIdentifierPreparer(dialect)
        super(AthenaDDLCompiler, self).__init__(
            dialect=dialect,
            statement=statement,
            bind=bind,
            schema_translate_map=schema_translate_map,
            compile_kwargs=compile_kwargs,
        )

    def visit_create_table(self, create):
        table = create.element
        preparer = self.preparer

        text = "\nCREATE EXTERNAL "
        text += "TABLE " + preparer.format_table(table) + " "
        text += "("

        separator = "\n"
        for create_column in create.columns:
            column = create_column.element
            try:
                processed = self.process(create_column)
                if processed is not None:
                    text += separator
                    separator = ", \n"
                    text += "\t" + processed
            except exc.CompileError as ce:
                util.raise_from_cause(
                    exc.CompileError(
                        util.u("(in table '{0}', column '{1}'): {2}").format(
                            table.description, column.name, ce.args[0]
                        )
                    )
                )

        # Athena does not support table constraint AFAIK
        # const = self.create_table_constraints(
        #     table,
        #     _include_foreign_key_constraints=create.include_foreign_key_constraints,
        # )
        # if const:
        #     text += separator + "\t" + const

        text += "\n)\n%s\n\n" % self.post_create_table(table)
        return text

    def post_create_table(self, table):
        dialect_kwargs = {**[values
                             for k, values in self.dialect.construct_arguments
                             if table.__class__ == k][0],
                          **{k.replace(f"{self.dialect.name}_", ''): v
                             for k, v in table.dialect_kwargs.items()
                             if k.startswith(f"{self.dialect.name}_")
                             }
                          }
        text = ""

        if table.comment:
            text += (
                " COMMENT "
                + process_comment_literal(
                    table.comment[:LIMIT_COMMENT_TABLE], self.dialect
                )
                + "\n"
            )
        partitioned_by = dialect_kwargs["partitioned_by"]
        if partitioned_by:
            text += _format_partitioned_by(partitioned_by) + "\n"

        row_format = dialect_kwargs["row_format"]
        stored_as = dialect_kwargs["stored_as"]
        if row_format and stored_as not in (_DEFAULT, "TEXTFILE"):
            # User explicitly specified conflicting args.
            raise exc.ArgumentError(
                "Conflicting dialect arguments: row_format & stored_as.",
                " You should only use one or the other",
            )
        stored_as = "TEXTFILE" if row_format else stored_as
        # From this point on, either row format is not set
        # or it is set and then store_as is either equal to TEXTFILE
        assert not row_format or stored_as == "TEXTFILE"
        assert not stored_as or (
            stored_as != "TEXTFILE" or (row_format and stored_as == "TEXTFILE")
        )

        if row_format:
            text += f"ROW FORMAT {row_format}\n"

        if stored_as:
            stored_as = "PARQUET" if stored_as is _DEFAULT else stored_as
            text += f"STORED AS {stored_as}\n"

        s3_dir = dialect_kwargs["s3_dir"].rstrip('/')
        if not s3_dir:
            raise exc.CompileError(
                "`s3_dir` parameter must be defined either at the table level"
                " as a dialect keyword argument or on the connection string."
            )

        s3_prefix = ''
        if table.schema:
            s3_prefix = table.schema
        else:
            # Should probably get rid of the "database" dialect kwargs and
            # use "database" as the default "s3_prefix" value.
            s3_prefix = dialect_kwargs["database"] or ''
        if dialect_kwargs['s3_prefix']:
            s3_prefix = dialect_kwargs['s3_prefix']

        s3_prefix = s3_prefix.strip('/')
        text += f"LOCATION '{s3_dir}{'/' if s3_prefix else ''}{s3_prefix}/{table.name}'\n"

        tblproperties = dialect_kwargs["tblproperties"]
        if tblproperties:
            text += _format_tblproperties(tblproperties) + "\n"
        # else:
        #     compression = raw_connection._kwargs.get("compression")
        #     if compression:
        #         text += "TBLPROPERTIES ('parquet.compress'='{0}')\n".format(
        #             compression.upper()
        #         )

        return text

    def get_column_specification(self, column, **kwargs):
        colspec = (
            self.preparer.format_column(column)
            + " "
            + self.dialect.type_compiler.process(column.type, type_expression=column)
        )
        # Athena does not support column default
        # default = self.get_column_default_string(column)
        # if default is not None:
        #     colspec += " DEFAULT " + default
        comment = ""
        if column.comment:
            comment = process_comment_literal(
                column.comment[:LIMIT_COMMENT_MEMBER],
                self.dialect,
                squash_blanks=True,
            )

        # I don't think Athena supports computed columns
        # if column.computed is not None:
        #     colspec += " " + self.process(column.computed)

        # Athena does not support column nullable constraint default
        # if not column.nullable:
        #     colspec += " NOT NULL"
        return f"{colspec}{' COMMENT ' if comment else ''}{comment}"


_TYPE_MAPPINGS = {
    "boolean": BOOLEAN,
    "real": FLOAT,
    "float": FLOAT,
    "double": FLOAT,
    "tinyint": INTEGER,
    "smallint": INTEGER,
    "integer": INTEGER,
    "bigint": BIGINT,
    "decimal": DECIMAL,
    "char": STRINGTYPE,
    "varchar": STRINGTYPE,
    "array": STRINGTYPE,
    "row": STRINGTYPE,  # StructType
    "varbinary": BINARY,
    "map": STRINGTYPE,
    "date": DATE,
    "timestamp": TIMESTAMP,
}


class AthenaDialect(DefaultDialect):

    name = "awsathena"
    driver = "rest"
    preparer = AthenaDMLIdentifierPreparer
    statement_compiler = AthenaStatementCompiler
    ddl_compiler = AthenaDDLCompiler
    type_compiler = AthenaTypeCompiler
    default_paramstyle = pyathena.paramstyle
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_multivalues_insert = True
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_native_boolean = True
    postfetch_lastrowid = False

    _pattern_data_catlog_exception = re.compile(
        r"(((Database|Namespace)\ (?P<schema>.+))|(Table\ (?P<table>.+)))\ not\ found\."
    )
    _pattern_column_type = re.compile(r"^([a-zA-Z]+)($|\(.+\)$)")
    construct_arguments: List[Tuple[DialectKWArgs, Mapping[str, Any]]] = [
        (
            schema.Table,
            {
                "database": None,  # TODO: only keep ``s3_prefix``
                "partitioned_by": None,
                "row_format": None,
                "s3_prefix": '',
                "s3_dir": None,
                "stored_as": _DEFAULT,
                "tblproperties": tuple(),
            },
        ),
    ]

    # Could help catch kwargs passed via create_engine(<URL>, s3_dir=..., s3_staging_dir=..., ...)
    def __init__(self, s3_dir=None, s3_prefix=None, s3_staging_prefix=None, *args, **kwargs):
        super().__init__(self, *args, **kwargs)
        construct_arguments = []
        for k, v in self.construct_arguments:
            if k is schema.Table and (s3_dir or s3_prefix):
                v = copy.copy(v)
                v['s3_prefix'] = s3_prefix or ''
                v['s3_dir'] = s3_dir
            construct_arguments.append((k, v, ))
        self.construct_arguments = construct_arguments

    @classmethod
    def dbapi(cls):
        return pyathena

    def _raw_connection(self, connection):
        if isinstance(connection, Engine):
            return connection.raw_connection()
        return connection.connection

    def _extract_dialect_kwargs_defaults(self, url):
        """Extracts dialect kwargs from the connection URL to overrides the instance's defaults

        The dialect provides defaults for all of its defined kwargs. This method
        allows to override the global defaults, on a per Dialect instance basis.

        Those can be further overrode, on a per constrcut basis.

        Limitations: if a same dialect kwargs name is used for several construct
        we assume those should have the same value. If the don't, they should be
        named differently.

        DOES NOT WORK :-( in that the SchemaItem.dialect_options() never receives the dialect
        instance and hence always fetches the dialect kwargs default value at the Dialect class
        level (check the diff between self.__class__.construct_arguments, self.construct_arguments)
        """
        kwargs = {'database': url.database, **url.query}
        new_construct_defaults = {}
        for construct, global_defaults in self.construct_arguments:
            new_construct_defaults[construct] = defaults = {}
            for k, v in global_defaults.items():
                if k in kwargs:
                    defaults[k] = kwargs.pop(k)
                else:
                    defaults[k] = v
        self.construct_arguments = list(new_construct_defaults.items())

    def create_connect_args(self, url):
        self._extract_dialect_kwargs_defaults(url)
        # Connection string format:
        #   awsathena+rest://
        #   {aws_access_key_id}:{aws_secret_access_key}@athena.{region_name}.amazonaws.com:443/
        #   {schema_name}?s3_staging_dir={s3_staging_dir}&...
        opts = {
            "aws_access_key_id": url.username if url.username else None,
            "aws_secret_access_key": url.password if url.password else None,
            "region_name": re.sub(
                r"^athena\.([a-z0-9-]+)\.amazonaws\.(com|com.cn)$", r"\1", url.host
            ),
            "schema_name": url.database if url.database else "default",
        }
        if "verify" in url.query:
            verify = url.query["verify"]
            try:
                verify = bool(strtobool(verify))
            except ValueError:
                # Probably a file name of the CA cert bundle to use
                pass
            url.query.update({"verify": verify})
        if "duration_seconds" in url.query:
            url.query.update({"duration_seconds": int(url.query["duration_seconds"])})
        if "poll_interval" in url.query:
            url.query.update({"poll_interval": float(url.query["poll_interval"])})
        if "kill_on_interrupt" in url.query:
            url.query.update(
                {"kill_on_interrupt": bool(strtobool(url.query["kill_on_interrupt"]))}
            )
        opts.update(url.query)
        return [[], opts]

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        query = """
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name NOT IN ('information_schema')
                """
        return [row.schema_name for row in connection.execute(query).fetchall()]

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        raw_connection = self._raw_connection(connection)
        schema = schema if schema else raw_connection.schema_name
        query = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = '{schema}'
                """.format(
            schema=schema
        )
        return [row.table_name for row in connection.execute(query).fetchall()]

    def has_table(self, connection, table_name, schema=None):
        try:
            columns = self.get_columns(connection, table_name, schema)
            return True if columns else False
        except NoSuchTableError:
            return False

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        raw_connection = self._raw_connection(connection)
        schema = schema if schema else raw_connection.schema_name
        query = """
                SELECT
                  table_schema,
                  table_name,
                  column_name,
                  data_type,
                  is_nullable,
                  column_default,
                  ordinal_position,
                  comment
                FROM information_schema.columns
                WHERE table_schema = '{schema}'
                AND table_name = '{table}'
                """.format(
            schema=schema, table=table_name
        )
        retry_config = raw_connection.retry_config
        retry = tenacity.Retrying(
            retry=retry_if_exception(
                lambda exc: self._retry_if_data_catalog_exception(
                    exc, schema, table_name
                )
            ),
            stop=stop_after_attempt(retry_config.attempt),
            wait=wait_exponential(
                multiplier=retry_config.multiplier,
                max=retry_config.max_delay,
                exp_base=retry_config.exponential_base,
            ),
            reraise=True,
        )
        try:
            return [
                {
                    "name": row.column_name,
                    "type": _TYPE_MAPPINGS.get(
                        self._get_column_type(row.data_type), NULLTYPE
                    ),
                    "nullable": True if row.is_nullable == "YES" else False,
                    "default": row.column_default
                    if not self._is_nan(row.column_default)
                    else None,
                    "ordinal_position": row.ordinal_position,
                    "comment": row.comment,
                }
                for row in retry(connection.execute, query).fetchall()
            ]
        except OperationalError as e:
            if not self._retry_if_data_catalog_exception(e, schema, table_name):
                raise NoSuchTableError(table_name) from e
            else:
                raise e

    def _retry_if_data_catalog_exception(self, exc, schema, table_name):
        if not isinstance(exc, OperationalError):
            return False

        match = self._pattern_data_catlog_exception.search(str(exc))
        if match and (
            match.group("schema") == schema or match.group("table") == table_name
        ):
            return False
        return True

    def _get_column_type(self, type_):
        return self._pattern_column_type.sub(r"\1", type_)

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        # Athena has no support for foreign keys.
        return []  # pragma: no cover

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        # Athena has no support for primary keys.
        return []  # pragma: no cover

    def get_indexes(self, connection, table_name, schema=None, **kw):
        # Athena has no support for indexes.
        return []  # pragma: no cover

    def do_rollback(self, dbapi_connection):
        # No transactions for Athena
        pass  # pragma: no cover

    def _check_unicode_returns(self, connection, additional_tests=None):
        # Requests gives back Unicode strings
        return True  # pragma: no cover

    def _check_unicode_description(self, connection):
        # Requests gives back Unicode strings
        return True  # pragma: no cover

    def _is_nan(self, column_default):
        return isinstance(column_default, numbers.Real) and math.isnan(column_default)
