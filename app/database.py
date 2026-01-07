# database.py
from sqlmodel import create_engine, Session
import os

DATABASE_URL = os.environ["DATABASE_URL"]
engine = create_engine(DATABASE_URL)

def get_session():
    with Session(engine) as session:
        yield session