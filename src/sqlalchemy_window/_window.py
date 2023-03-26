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
from sqlalchemy.sql.elements import _OverRange
from sqlalchemy.sql.roles import ByOfRole

if typing.TYPE_CHECKING:  # pragma: no coverage
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


class FrameExclude(enum.Enum):
    CURRENT_ROW = "CURRENT ROW"
    GROUP = "GROUP"
    TIES = "TIES"
    NO_OTHERS = "NO OTHERS"


CURRENT_ROW = FrameExclude.CURRENT_ROW
GROUP = FrameExclude.GROUP
TIES = FrameExclude.TIES
NO_OTHERS = FrameExclude.NO_OTHERS


class Window(ClauseElement):
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

        if existing_window is not None and existing_window.partition_by is not None:
            self.partition_by = existing_window.partition_by
        elif partition_by is not None:
            self.partition_by = ClauseList(
                *util.to_list(partition_by),
                _literal_as_text_role=ByOfRole,
            )

        if existing_window is not None and existing_window.order_by is not None:
            self.order_by = existing_window.order_by
        elif order_by is not None:
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
        if (value := getattr(element, attr)) is not None:
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
    return "{} AS ({})".format(element.name, text)
