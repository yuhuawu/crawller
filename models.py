from typing import Optional
from sqlmodel import Field, SQLModel
from datetime import datetime, timezone

class StockPrice(SQLModel, table=True):
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    symbol: str = Field(index=True)
    date: datetime = Field(index=True)
    open: float
    high: float
    low: float
    close: float
    volume: int

# Define SQLModel for task status
class TaskStatus(SQLModel, table=True):
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    total_tasks: int = 0
    completed_tasks: int = 0
    failed_tasks: int = 0