from abc import ABC, abstractmethod

from stockodile.schema.records import Record


class Sink(ABC):
    @abstractmethod
    async def put(self, record: Record) -> None:
        """Buffer a record; auto-flush if thresholds are exceeded."""
        ...

    @abstractmethod
    async def flush(self) -> None:
        """Flush all buffered channels to Parquet."""
        ...

    async def close(self) -> None:
        """Close the sink, flushing any remaining records."""
        await self.flush()


class MemorySink(Sink):
    def __init__(self) -> None:
        self.records: list[Record] = []

    async def put(self, record: Record) -> None:
        self.records.append(record)

    async def flush(self) -> None:
        pass
