# -*- coding: utf-8; -*-
from pyathena.sqlalchemy_athena import AthenaDialect


def test_dialect_kwargs(dialect_overrides):
    # Given
    class_dialect_kwargs = [e[1] for e in AthenaDialect.construct_arguments if e[0] is Table][0]

    # When
    dialect = AthenaDialect(**dialect_overrides)
    instance_dialect_kwargs = [e[1] for e in dialect.construct_arguments if e[0] is Table][0]

    # Then
    assert AthenaDialect.construct_arguments is not dialect.construct_arguments
    assert all(instance_dialect_kwargs[k] == v for k, v in dialect_overrides.items())
    assert all(class_dialect_kwargs[k] != instance_dialect_kwargs[k]
               for k in dialect_overrides)


# vim: et:sw=4:syntax=python:ts=4:
