from sqlalchemy import select , insert , inspect , Table , Column , Float , Integer , String , DateTime , Boolean , create_engine , text
from sqlalchemy.orm import sessionmaker , Session
from sqlalchemy.ext.declarative import declarative_base
import pymysql
from datetime import datetime, timezone
from fastapi import Depends

Base = declarative_base()

class Users(Base):
    __tablename__ = "users"

    username = Column(String(100) , primary_key = True , nullable = False)
    password = Column(String(100) , nullable = False)
    is_admin = Column(Boolean , nullable = False , server_default = text("0"))
    registered_date = Column(DateTime , nullable = False)

class Logs(Base):
    __tablename__ = "logs"

    username = Column(String(100) , primary_key = True , nullable = False)
    log_info = Column(String(100) , nullable = False)
    log_date = Column(DateTime , nullable = False)

class Predictions(Base):
    __tablename__ = "predictions"

    username = Column(String(100) , primary_key = True , nullable = False)
    home_team = Column(String(100) , nullable = False)
    away_team = Column(String(100) , nullable = False)
    game_date = Column(DateTime , nullable = False)
    home_odd = Column(Float , nullable = False)
    draw_odd = Column(Float , nullable = False)
    away_odd = Column(Float , nullable = False)
    prediction_date = Column(DateTime , nullable = False)

engine = create_engine("mysql+pymysql://Mathieu:A4xpgru+@localhost/project")

if not inspect(engine).has_table("users") :
    Base.metadata.create_all(bind = engine)

def start_session():
    sm = sessionmaker(engine)
    session = sm()   
    try:
        yield session
    finally:
        session.close()

def current_user(username : str , session : Session):
    statement = select(Users).filter_by(username = username)
    return session.scalars(statement).first()

def add_to_users_table(user : Users , session : Session):
    session.add(user)
    session.commit()

def add_to_predictions_table(prediction : Predictions , session : Session):
    session.add(prediction)
    session.commit()

def add_to_logs_table(log : Logs , session : Session):
    session.add(log)
    session.commit()