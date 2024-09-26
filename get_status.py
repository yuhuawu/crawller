from sqlmodel import Session, select, create_engine
from models import TaskStatus
import time

def read_status(engine):
    with Session(engine) as session:
        status = session.exec(select(TaskStatus)).first()
        if status:
            total = status.total_tasks
            completed = status.completed_tasks
            failed = status.failed_tasks
            print(f"Progress: {total} total, {completed} completed, {failed} failed")
        else:
            print("No status information available")

def main():
    engine = create_engine('sqlite:///stock_data.db')
    while True:
        read_status(engine)
        time.sleep(5)  # Read status every 5 seconds

if __name__ == "__main__":
    main()