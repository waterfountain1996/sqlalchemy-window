import enum
import typing

from sqlalchemy import util
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.compiler import SQLCompiler
from sqlalchemy.sql.elements import RANGE_CURRENT
from sqlalchemy.sql.elements import RANGE_UNBOUNDED
from sqlalchemy.sql.elements import ClauseElement
from sqlalchemy.sql.elements import ClauseList
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.elements import _OverRange
from sqlalchemy.sql.functions import FunctionElement
from sqlalchemy.sql.roles import ByOfRole

if typing.TYPE_CHECKING:  # pragma: no cover
    from sqlalchemy.sql._typing import _ColumnExpressionArgument

_ColumnExpression = typing.Union[
    "typing.Iterable[_ColumnExpressionArgument[typing.Any]]",
    "_ColumnExpressionArgument[typing.Any]",
]

_RangeArgument = typing.Union[
    range,
    typing.Tuple[typing.Optional[int], typing.Optional[int]],
]

_RangeType = typing.Union[_OverRange, int]

_FT = typing.TypeVar("_FT")


class FrameExclude(enum.Enum):
    CURRENT_ROW = "CURRENT ROW"
    GROUP = "GROUP"
    TIES = "TIES"
    NO_OTHERS = "NO OTHERS"


CURRENT_ROW = FrameExclude.CURRENT_ROW
GROUP = FrameExclude.GROUP
TIES = FrameExclude.TIES
NO_OTHERS = FrameExclude.NO_OTHERS


class OverWindow(ColumnElement[_FT]):
    """Represent an `OVER` statement for window functions.

    Do not construct directly, rather through a
    `sqlalchemy_window.over_window` factory.
    """

    def __init__(self, element: FunctionElement[_FT], window: "Window") -> None:
        self.element = element
        self.window = window

    @property
    def type(self):
        return self.element.type


def over_window(element: FunctionElement[_FT], window: "Window") -> OverWindow[_FT]:
    """Construct an `OverWindow` object.

    Built-in `.over()` method on SQLAlchemy's window function can only
    be used to specify an inline window clause. However if you want to
    reuse one window in multiple places you can use this.

    Example usage:

    ```
        w = window("w", ...)
        over_window(func.first_value(literal_column("foo")), w)
    ```

    This roughly compiles to following SQL:

        `first_value(foo) OVER w`
    """
    return OverWindow(element, window)


class Window(ClauseElement):
    """Represent a WINDOW expression.

    This is a special construct to allow reuse of the same
    options for window function in multiple places.

    Do not use this element directly, rather through a
    `sqlalchemy_window.window` factory.

    For more information, see:
    https://www.postgresql.org/docs/current/sql-select.html#SQL-WINDOW
    """

    partition_by: typing.Optional[ClauseList] = None
    order_by: typing.Optional[ClauseList] = None

    range_: typing.Optional[_RangeType] = None
    rows: typing.Optional[_RangeType] = None
    groups: typing.Optional[_RangeType] = None

    def __init__(
        self,
        name: str,
        existing_window: typing.Optional["Window"] = None,
        partition_by: typing.Optional[_ColumnExpression] = None,
        order_by: typing.Optional[_ColumnExpression] = None,
        range_: typing.Optional[_RangeArgument] = None,
        rows: typing.Optional[_RangeArgument] = None,
        groups: typing.Optional[_RangeArgument] = None,
        exclude: typing.Optional[FrameExclude] = None,
    ) -> None:
        self.name = name
        self.existing_window = existing_window

        if partition_by is not None:
            if existing_window is not None:
                raise ArgumentError(
                    f"Cannot override PARTITION BY clause of window '{existing_window.name}'"
                )

            self.partition_by = ClauseList(
                *util.to_list(partition_by),
                _literal_as_text_role=ByOfRole,
            )

        if order_by is not None:
            if existing_window is not None and existing_window.order_by is not None:
                raise ArgumentError(
                    f"Cannot override ORDER BY clause of window {existing_window.name}"
                )

            self.order_by = ClauseList(
                *util.to_list(order_by),
                _literal_as_text_role=ByOfRole,
            )

        if sum(o is not None for o in (range_, rows, groups)) > 1:
            raise ArgumentError("'range_', 'rows' and 'groups' are mutually exclusive")

        for attr, value in (("range_", range_), ("rows", rows), ("groups", groups)):
            if value is not None:
                setattr(self, attr, self._normalize_range(value))

        if exclude is not None and not isinstance(exclude, FrameExclude):
            raise ArgumentError("'exclude' must be an instance of FrameExclude")

        self.exclude = exclude

    @staticmethod
    def _normalize_range(range_: _RangeArgument) -> typing.Tuple[_RangeType, _RangeType]:
        if isinstance(range_, range):
            range_ = (range_.start, range_.stop)

        if not isinstance(range_, tuple) or len(range_) != 2:
            raise ArgumentError("2-tuple expected for range/rows/groups")

        def normalize_boundary(b: typing.Any) -> _RangeType:
            if b is None:
                return RANGE_UNBOUNDED

            try:
                b = int(b)
            except ValueError as e:
                raise ArgumentError("int or None expected for range value") from e
            else:
                return RANGE_CURRENT if b == 0 else b

        lower, upper = map(normalize_boundary, range_)
        return lower, upper

    def over_self(self, element: FunctionElement[_FT]) -> OverWindow[_FT]:
        """Construct an `OverWindow` object from a given function and self."""
        return over_window(element, self)


def window(
    name: str,
    existing_window: typing.Optional[Window] = None,
    partition_by: typing.Optional[_ColumnExpression] = None,
    order_by: typing.Optional[_ColumnExpression] = None,
    range_: typing.Optional[_RangeArgument] = None,
    rows: typing.Optional[_RangeArgument] = None,
    groups: typing.Optional[_RangeArgument] = None,
    exclude: typing.Optional[FrameExclude] = None,
) -> Window:
    """Construct a `Window` object.

    It can be then passed to `sqlalchemy_window.Select` object
    to build a WINDOW clause.

    Arguments to this function try to mimic actual options passed
    to a WINDOW clause in SELECT statement.

    Example usage:

        `w = window("w", partition_by=literal_column("foo"))`

    This roughly compiles to following SQL:

        `w AS (PARTITION BY foo)`

    `existing_window`: A window to inherit from. When passed to
    `sqlalchemy_window.Select.window` must come before this one.
    If provided, you can't specify a partition_by parameter or
    it'll raise a `sqlalchemy.exc.ArgumentError` exception no matter
    whether the `partition_by` of the `existing_window` is set or not.
    However, you can pass an `order_by` parameter ONLY if the
    `existing_window` does NOT specify it.

    `partition_by`: Column(s) to partition the rolling window by.

    `order_by`: Column(s) to order the rows inside the window by.

    `range_`, `rows`, `groups`: An optional window frame.
    Exactly one of these arguments must be provided, otherwise
    an `sqlalchemy.exc.ArgumentError` is raised.
    It can either be a `range` object or a 2-element tuple.
    If a tuple is used, its items can either be an it or None.
    If None is used, it will be rendered as
    `UNBOUNDED PRECEDING/FOLLOWING`.
    If int is used: 0 renders as `'CURRENT ROW'`,
    negative number renders AS `'N PRECEDING'`
    and a positive as `'N FOLLOWING'`.

    `exclude`: `EXCLUDE` option inside the frame clause.
    Only used if either of `range_`, `rows`, `groups` is specified.
    """
    return Window(
        name=name,
        existing_window=existing_window,
        partition_by=partition_by,
        order_by=order_by,
        range_=range_,
        rows=rows,
        groups=groups,
        exclude=exclude,
    )


@compiles(OverWindow)
def compile_over_window(element: OverWindow, compiler: SQLCompiler, **kwargs: typing.Any) -> str:
    return "{} OVER {}".format(compiler.process(element.element), element.window.name)


@compiles(Window, "postgresql")
def compile_window(element: Window, compiler: SQLCompiler, **kwargs: typing.Any) -> str:
    def format_frame_clause(
        range_: typing.Tuple[typing.Union[typing.Any, int], typing.Union[typing.Any, int]]
    ) -> str:
        return "{} AND {}".format(
            "UNBOUNDED PRECEDING"
            if range_[0] is RANGE_UNBOUNDED
            else "CURRENT ROW"
            if range_[0] is RANGE_CURRENT
            else "{} PRECEDING".format(abs(range_[0]))
            if range_[0] < 0
            else "{} FOLLOWING".format(abs(range_[0])),
            "UNBOUNDED FOLLOWING"
            if range_[1] is RANGE_UNBOUNDED
            else "CURRENT ROW"
            if range_[1] is RANGE_CURRENT
            else "{} PRECEDING".format(abs(range_[1]))
            if range_[1] < 0
            else "{} FOLLOWING".format(abs(range_[1])),
        )

    frame: typing.Optional[str] = None
    for word, attr in (("RANGE", "range_"), ("ROWS", "rows"), ("GROUPS", "groups")):
        value = getattr(element, attr, None)
        if value is not None:
            frame = "{} BETWEEN {}".format(word, format_frame_clause(value))

    if frame and element.exclude:
        frame += " EXCLUDE {}".format(element.exclude.value)

    text = " ".join(
        [
            "{} BY {}".format(word, compiler.process(clause))
            for word, clause in (
                ("PARTITION", element.partition_by),
                ("ORDER", element.order_by),
            )
            if clause is not None and len(clause) > 0
        ]
        + ([frame] if frame else [])
    )

    if element.existing_window is not None:
        text = "{} {}".format(element.existing_window.name, text)

    return "{} AS ({})".format(element.name, text)
