import typing

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.compiler import SQLCompiler
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.functions import FunctionElement

from ._window import Window

_T = typing.TypeVar("_T")


class OverWindow(ColumnElement[_T]):
    """Represent an `OVER` statement for window functions.

    Do not construct directly, rather through a
    `sqlalchemy_window.over_window` factory.
    """

    def __init__(self, element: FunctionElement[_T], window: Window) -> None:
        self.element = element
        self.window = window

    @property
    def type(self):
        return self.element.type


def over_window(element: FunctionElement[_T], window: Window) -> OverWindow[_T]:
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


@compiles(OverWindow)
def compile_over_window(element: OverWindow, compiler: SQLCompiler, **kwargs: typing.Any) -> str:
    return "{} OVER {}".format(compiler.process(element.element), element.window.name)
