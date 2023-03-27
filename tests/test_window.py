import typing
from itertools import combinations
from unittest.mock import Mock
from unittest.mock import patch

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import ArgumentError
from sqlalchemy.sql import literal_column

from sqlalchemy_window import CURRENT_ROW
from sqlalchemy_window import Window
from sqlalchemy_window import window


def compile_window(w: Window) -> str:
    """Compile window expression to string"""
    return str(w.compile(dialect=postgresql.dialect()))


def test_override_partition_by_of_existing_window_without_partition_by_clause():
    existing = Window("existing")
    with pytest.raises(ArgumentError) as ctx:
        Window("w", existing_window=existing, partition_by=literal_column("bar"))

    assert "override PARTITION BY" in str(ctx.value)


def test_override_partition_by_of_existing_window():
    existing = Window("existing", partition_by=literal_column("foo"))
    with pytest.raises(ArgumentError) as ctx:
        Window("w", existing_window=existing, partition_by=literal_column("bar"))

    assert "override PARTITION BY" in str(ctx.value)


def test_order_by_with_existing_window_without_order_by_clause():
    existing = Window("existing")
    w = Window("w", existing_window=existing, order_by=literal_column("bar"))
    assert compile_window(w) == "w AS (existing ORDER BY bar)"


def test_override_order_by_of_existing_window():
    existing = Window("existing", order_by=literal_column("foo"))
    with pytest.raises(ArgumentError) as ctx:
        Window("w", existing_window=existing, order_by=literal_column("bar"))

    assert "override ORDER BY" in str(ctx.value)


def test_mutually_exclusive_parameters():
    params = ("range_", "rows", "groups")
    for args in combinations(params, 2):
        with pytest.raises(ArgumentError) as ctx:
            Window("w", **{a: (None, None) for a in args})

        assert "mutually exclusive" in str(ctx.value)


def test_pass_range():
    value = Mock()
    with patch.object(Window, "_normalize_range", return_value=value):
        w = Window("w", range_=(None, None))
        assert w.range_ is value
        assert w.rows is None
        assert w.groups is None


def test_pass_rows():
    value = Mock()
    with patch.object(Window, "_normalize_range", return_value=value):
        w = Window("w", rows=(None, None))
        assert w.range_ is None
        assert w.rows is value
        assert w.groups is None


def test_pass_groups():
    value = Mock()
    with patch.object(Window, "_normalize_range", return_value=value):
        w = Window("w", groups=(None, None))
        assert w.range_ is None
        assert w.rows is None
        assert w.groups is value


def test_invalid_exclude():
    with pytest.raises(ArgumentError) as ctx:
        Window("w", exclude="foobar")  # type: ignore

    assert "'exclude'" in str(ctx.value)


def test_missing_exclude():
    w = Window("w", exclude=None)
    assert w.exclude is None


def test_compile_with_no_parameters():
    w = Window("w")
    assert compile_window(w) == "w AS ()"


def test_compile_with_partition_by():
    w = Window("w", partition_by=literal_column("foo"))
    assert compile_window(w) == "w AS (PARTITION BY foo)"


def test_compile_with_order_by():
    w = Window("w", order_by=literal_column("foo"))
    assert compile_window(w) == "w AS (ORDER BY foo)"


def test_compile_with_partition_by_and_order_by():
    w = Window("w", partition_by=literal_column("foo"), order_by=literal_column("bar"))
    assert compile_window(w) == "w AS (PARTITION BY foo ORDER BY bar)"


@pytest.mark.parametrize(
    ("range_", "result"),
    [
        ((-5, 10), "RANGE BETWEEN 5 PRECEDING AND 10 FOLLOWING"),
        ((None, 0), "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
        ((-2, None), "RANGE BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING"),
        ((1, 3), "RANGE BETWEEN 1 FOLLOWING AND 3 FOLLOWING"),
    ],
)
def test_compile_range(range_: typing.Tuple[int, int], result: str):
    w = Window("w", range_=range_)
    assert result in compile_window(w)


@pytest.mark.parametrize(
    ("rows", "result"),
    [
        (range(-3, 2), "ROWS BETWEEN 3 PRECEDING AND 2 FOLLOWING"),
        (range(10), "ROWS BETWEEN CURRENT ROW AND 10 FOLLOWING"),
        ((-2, None), "ROWS BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING"),
        ((1, 3), "ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING"),
    ],
)
def test_compile_rows(rows: typing.Tuple[int, int], result: str):
    w = Window("w", rows=rows)
    assert result in compile_window(w)


@pytest.mark.parametrize(
    ("groups", "result"),
    [
        ((-5, 10), "GROUPS BETWEEN 5 PRECEDING AND 10 FOLLOWING"),
        ((None, 0), "GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
        ((-2, None), "GROUPS BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING"),
        ((1, 3), "GROUPS BETWEEN 1 FOLLOWING AND 3 FOLLOWING"),
    ],
)
def test_compile_groups(groups: typing.Tuple[int, int], result: str):
    w = Window("w", groups=groups)
    assert result in compile_window(w)


def test_compile_range_with_exclude():
    w = Window("w", range_=(None, None), exclude=CURRENT_ROW)
    assert "EXCLUDE CURRENT ROW" in compile_window(w)


@pytest.mark.parametrize(("range_"), [("foobar",), ((1, 2, 3),)])
def test_compile_incorrect_range(range_: typing.Any):
    with pytest.raises(ArgumentError) as ctx:
        Window("w", range_=range_)

    assert "2-tuple expected" in str(ctx.value)


def test_compile_incorrect_range_value():
    with pytest.raises(ArgumentError) as ctx:
        Window("w", range_=(1, "foo"))  # type: ignore

    assert "int or None expected" in str(ctx.value)


def test_window_factory():
    w = window("w")
    assert isinstance(w, Window)
