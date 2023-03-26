import typing

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.compiler import SQLCompiler
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.functions import FunctionElement

from ._window import Window

_T = typing.TypeVar("_T")


class OverWindow(ColumnElement[_T]):
    def __init__(self, element: FunctionElement[_T], window: Window) -> None:
        self.element = element
        self.window = window

    @property
    def type(self):
        return self.element.type


def over_window(element: FunctionElement[_T], window: Window) -> OverWindow[_T]:
    return OverWindow(element, window)


@compiles(OverWindow)
def compile_over_window(element: OverWindow, compiler: SQLCompiler, **kwargs: typing.Any) -> str:
    return "{} OVER {}".format(compiler.process(element.element), element.window.name)
