import asyncio
import aiohttp
import yfinance as yf
from sqlmodel import Field, Session, SQLModel, create_engine, select
from models import TaskStatus, StockPrice
from typing import Optional, List
from datetime import datetime, timedelta
import pandas as pd

class StockDataFetcher:
    def __init__(self, db_url='sqlite:///stock_data.db'):
        self.engine = create_engine(db_url)
        self.create_db_and_tables()
        self.session = Session(self.engine)
        self.init_task_status()

    def create_db_and_tables(self):
        SQLModel.metadata.create_all(self.engine)

    def init_task_status(self):
        status = self.session.exec(select(TaskStatus)).first()
        if not status:
            status = TaskStatus()
            self.session.add(status)
            self.session.commit()

    def update_task_status(self, total=0, completed=0, failed=0):
        status = self.session.exec(select(TaskStatus)).first()
        status.total_tasks += total
        status.completed_tasks += completed
        status.failed_tasks += failed
        self.session.commit()

    async def fetch_all_symbols(self):
        url = "https://pkgstore.datahub.io/core/nasdaq-listings/nasdaq-listed_json/data/a5bc7580d6176d60ac0b2142ca8d7df6/nasdaq-listed_json.json"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                data = await response.json()
                return [item['Symbol'] for item in data]

    async def fetch_stock_data(self, symbol: str, start_date: str, end_date: str):
        try:
            stock = yf.Ticker(symbol)
            data = await asyncio.to_thread(stock.history, start=start_date, end=end_date)
            
            stock_prices = [
                StockPrice(
                    symbol=symbol,
                    date=index.to_pydatetime(),
                    open=row['Open'],
                    high=row['High'],
                    low=row['Low'],
                    close=row['Close'],
                    volume=row['Volume']
                )
                for index, row in data.iterrows()
            ]

            await self.insert_data(stock_prices)
            self.update_task_status(completed=1)
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
            self.update_task_status(failed=1)

    async def insert_data(self, stock_prices: List[StockPrice]):
        def db_insert():
            with Session(self.engine) as session:
                for price in stock_prices:
                    stmt = select(StockPrice).where(StockPrice.symbol == price.symbol, StockPrice.date == price.date)
                    existing = session.exec(stmt).first()
                    if existing:
                        for field in ['open', 'high', 'low', 'close', 'volume']:
                            setattr(existing, field, getattr(price, field))
                    else:
                        session.add(price)
                session.commit()

        await asyncio.to_thread(db_insert)

    async def fetch_all_stocks(self, start_date: str, end_date: str):
        symbols = await self.fetch_all_symbols()
        self.update_task_status(total=len(symbols))
        
        chunks = [symbols[i:i + 100] for i in range(0, len(symbols), 100)]
        for chunk in chunks:
            tasks = [self.fetch_stock_data(symbol, start_date, end_date) for symbol in chunk]
            await asyncio.gather(*tasks)