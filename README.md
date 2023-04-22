# sqlalchemy_window

A SQLAlchemy plugin to add support for PostgreSQL WINDOW clause.

**NOTE**: only supports SQLAlchemy 2.0 and higher.

## Example

Newer versions (>= 0.1.3) allow you to use `Window.over_self` interface
for window functions:

```py
import sqlalchemy as sa
from sqlalchemy_window import over_window, select, window

metadata = sa.MetaData()
table = sa.Table(
  "prices",
  metadata,
  sa.Column("asset", sa.String(16), primary_key=True),
  sa.Column("ts", sa.DateTime(timezone=True), primary_key=True),
  sa.Column("price", sa.Numeric, nullable=False),
)

w = window("w", partition_by=table.c["asset"], order_by=table.c["ts"], range_=(None, None))

query = select(
  w.over_self(sa.func.first_value(table.c["price"])).label("open"),
  w.over_self(sa.func.max(table.c["price"])).label("high"),
  w.over_self(sa.func.min(table.c["price"])).label("low"),
  w.over_self(sa.func.last_value(table.c["price"])).label("close"),
).where(sa.func.cast(table.c["ts"], sa.Date) == '2023-01-01').window(w)
```

Before that, to build above query you would use `over_window` factory:

```py
query = select(
  over_window(sa.func.first_value(table.c["price"]), w).label("open"),
  over_window(sa.func.max(table.c["price"]), w).label("high"),
  over_window(sa.func.min(table.c["price"]), w).label("low"),
  over_window(sa.func.last_value(table.c["price"]), w).label("close"),
).where(sa.func.cast(table.c["ts"], sa.Date) == '2023-01-01').window(w)
```

You can still use `over_window` in newer versions.

## Development

To setup a development environment run:

```bash
python3 -m venv venv
source ./venv/bin/activate
pip install --upgrade pip
pip install -r dev-requirements.txt -e .
pre-commit install
```

Running tests:

```bash
make test
make coverage
```

---

A waterfountain1996 project.
