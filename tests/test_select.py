from sqlalchemy import func
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import literal
from sqlalchemy.sql import literal_column

from sqlalchemy_window import over_window
from sqlalchemy_window import select
from sqlalchemy_window import window


def test_select_with_one_window():
    s = select(literal("foo").label("bar")).window(window("w"))
    assert str(
        s.compile(dialect=postgresql.dialect(), compile_kwargs=dict(literal_binds=True))
    ) == ("SELECT 'foo' AS bar \n" "WINDOW w AS ()")


def test_select_with_multiple_windows():
    s = select(literal("foo").label("bar")).window(
        window("w1", partition_by=literal_column("col1")),
        window("w2", order_by=literal_column("col2")),
        window("w3", range_=(None, None)),
    )
    assert str(
        s.compile(dialect=postgresql.dialect(), compile_kwargs=dict(literal_binds=True))
    ) == (
        "SELECT 'foo' AS bar \n"
        "WINDOW w1 AS (PARTITION BY col1), "
        "w2 AS (ORDER BY col2), "
        "w3 AS (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
    )


def test_select_over_window():
    w = window("w", partition_by=literal_column("asset"), order_by=literal_column("tim"))
    s = select(
        over_window(func.first_value(literal_column("price")), w).label("open"),
        over_window(func.last_value(literal_column("price")), w).label("close"),
    ).window(w)
    assert str(
        s.compile(dialect=postgresql.dialect(), compile_kwargs=dict(literal_binds=True))
    ) == (
        "SELECT first_value(price) OVER w AS open, last_value(price) OVER w AS close \n"
        "WINDOW w AS (PARTITION BY asset ORDER BY tim)"
    )


def test_select_inherit_window():
    existing = window("inner_w", partition_by=literal_column("col1"))
    s = select(literal("foo").label("bar")).window(
        existing,
        window(
            "w",
            existing_window=existing,
            order_by=literal_column("col2"),
        ),
    )
    assert str(
        s.compile(dialect=postgresql.dialect(), compile_kwargs=dict(literal_binds=True))
    ) == (
        "SELECT 'foo' AS bar \n"
        "WINDOW inner_w AS (PARTITION BY col1), w AS (inner_w ORDER BY col2)"
    )
