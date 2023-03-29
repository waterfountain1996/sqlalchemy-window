import typing

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.compiler import COLLECT_CARTESIAN_PRODUCTS
from sqlalchemy.sql.compiler import WARN_LINTING
from sqlalchemy.sql.compiler import FromLinter
from sqlalchemy.sql.compiler import SQLCompiler
from sqlalchemy.sql.expression import Select as _Select

from ._window import Window

if typing.TYPE_CHECKING:  # pragma: no cover
    from sqlalchemy.sql._typing import _ColumnsClauseArgument


class Select(_Select):
    """Custom `Select` construct with WINDOW clause support."""

    _window_clause: typing.Tuple[Window, ...] = ()

    def window(self, *windows: Window) -> "Select":
        """Return a new `Select` object with extended WINDOW clause."""
        assert isinstance(self._window_clause, tuple)
        self._window_clause += windows
        return self


def select(*entities: "_ColumnsClauseArgument[typing.Any]") -> Select:
    """Construct a `Select` object with window support.

    Example usage:

    ```
        w = Window("w", ...)
        select(
            over_window( func.max(literal_column("foo")), w).label("bar")
        ).window(w)
    ```

    This would then produce roughly following SQL:

    ```
        SELECT max(foo) OVER w AS BAR
        WINDOW w AS (...)
    ```
    """
    return Select(*entities)


@compiles(Select, "postgresql")
def compile_select(element: Select, compiler: SQLCompiler, **kwargs: typing.Any) -> str:
    # This function is copied directly from SQLCompiler
    def _compose_select_body(
        text: str,
        select: Select,
        compile_state,
        inner_columns,
        froms,
        byfrom,
        toplevel,
        kwargs: typing.Dict[str, typing.Any],
    ) -> str:  # pragma: no cover
        text += ", ".join(inner_columns)

        if compiler.linting & COLLECT_CARTESIAN_PRODUCTS:
            from_linter = FromLinter({}, set())
            warn_linting = compiler.linting & WARN_LINTING
            if toplevel:
                compiler.from_linter = from_linter
        else:
            from_linter = None
            warn_linting = False

        # adjust the whitespace for no inner columns, part of #9440,
        # so that a no-col SELECT comes out as "SELECT WHERE..." or
        # "SELECT FROM ...".
        # while it would be better to have built the SELECT starting string
        # without trailing whitespace first, then add whitespace only if inner
        # cols were present, this breaks compatibility with various custom
        # compilation schemes that are currently being tested.
        if not inner_columns:
            text = text.rstrip()

        if froms:
            text += " \nFROM "

            if select._hints:
                text += ", ".join(
                    [
                        f._compiler_dispatch(
                            compiler,
                            asfrom=True,
                            fromhints=byfrom,
                            from_linter=from_linter,
                            **kwargs,
                        )
                        for f in froms
                    ]
                )
            else:
                text += ", ".join(
                    [
                        f._compiler_dispatch(
                            compiler,
                            asfrom=True,
                            from_linter=from_linter,
                            **kwargs,
                        )
                        for f in froms
                    ]
                )
        else:
            text += compiler.default_from()

        if select._where_criteria:
            t = compiler._generate_delimited_and_list(
                select._where_criteria, from_linter=from_linter, **kwargs
            )
            if t:
                text += " \nWHERE " + t

        if warn_linting:
            assert from_linter is not None
            from_linter.warn()

        if select._group_by_clauses:
            text += compiler.group_by_clause(select, **kwargs)

        if select._having_criteria:
            t = compiler._generate_delimited_and_list(select._having_criteria, **kwargs)
            if t:
                text += " \nHAVING " + t

        # Here's the logic for rendering a WINDOW clause.
        # 'select' is a freshly composed base Select object
        # so we use 'element' to get the original window clause.
        if hasattr(element, "_window_clause") and element._window_clause:
            t = ", ".join(compiler.process(window, **kwargs) for window in element._window_clause)
            if t:
                text += " \nWINDOW " + t

        if select._order_by_clauses:
            text += compiler.order_by_clause(select, **kwargs)

        if select._has_row_limiting_clause:
            text += compiler._row_limit_clause(select, **kwargs)

        if select._for_update_arg is not None:
            text += compiler.for_update_clause(select, **kwargs)

        return text

    # Save original method
    og_compose_select_body = compiler._compose_select_body

    # Monkey patch the method
    compiler._compose_select_body = _compose_select_body
    text = compiler.visit_select(element, **kwargs)

    # Restore original version
    compiler._compose_select_body = og_compose_select_body
    return text
