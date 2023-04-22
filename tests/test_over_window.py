from sqlalchemy import func
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import literal_column
from sqlalchemy.sql import select

from sqlalchemy_window import OverWindow
from sqlalchemy_window import over_window
from sqlalchemy_window import window


def test_over_window():
    w = window("w")
    o = OverWindow(func.first_value(literal_column("foo")), w)
    assert str(o.compile(dialect=postgresql.dialect())) == "first_value(foo) OVER w"


def test_select_over_window_with_label():
    w = window("w")
    s = select(
        over_window(
            func.first_value(literal_column("foo")),
            w,
        ).label("bar")
    )
    assert str(s.compile(dialect=postgresql.dialect())) == "SELECT first_value(foo) OVER w AS bar"


def test_window_over_self():
    w = window("w")
    f = func.sum()
    assert str(w.over_self(f).compile(dialect=postgresql.dialect())) == "sum() OVER w"


def test_select_window_over_self():
    w = window("w")
    s = select(w.over_self(func.first_value(literal_column("foo"))).label("bar"))
    assert str(s.compile(dialect=postgresql.dialect())) == "SELECT first_value(foo) OVER w AS bar"
