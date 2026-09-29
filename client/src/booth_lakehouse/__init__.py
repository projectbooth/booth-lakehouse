"""Project Booth's lakehouse client: Iceberg tables in a workspace, with simple commands.

    from booth_lakehouse import Lakehouse
    lh = Lakehouse.from_env()
    lh.create_table("sales.daily", df)
    lh.append("sales.daily", more)
    lh.read("sales.daily").to_pandas()

See ``lakehouse.py`` for how the catalog, storage credentials (ADR 0080) and writes (PyIceberg,
ADR 0079) fit together.
"""

from .broker import READ, READWRITE, Broker, BrokerError, HttpBroker, S3Grant
from .lakehouse import Lakehouse, LakehouseError, LakehouseTable

__all__ = [
    "READ",
    "READWRITE",
    "Broker",
    "BrokerError",
    "HttpBroker",
    "Lakehouse",
    "LakehouseError",
    "LakehouseTable",
    "S3Grant",
]
__version__ = "0.1.0"
