import types

import pytest


class FakeField:
    """Campo de schema com a mesma interface consumida pelos geradores de SQL."""

    def __init__(self, name, type_name, **attrs):
        self.name = name
        self.dataType = type(type_name, (), attrs)()


class FakeDataFrame:
    """DataFrame mínimo: os geradores de SQL só usam schema.fields e columns."""

    def __init__(self, fields):
        self.schema = types.SimpleNamespace(fields=list(fields))
        self.columns = [f.name for f in fields]


@pytest.fixture
def make_field():
    def _make(name, type_name, **attrs):
        return FakeField(name, type_name, **attrs)

    return _make


@pytest.fixture
def make_df(make_field):
    def _make(*fields):
        return FakeDataFrame(fields)

    return _make
