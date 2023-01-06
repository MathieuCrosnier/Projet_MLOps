from sqlalchemy import select , insert , inspect , Table , Column , Float , Integer , String , DateTime , Boolean , create_engine , text
from sqlalchemy.orm import sessionmaker , Session
from sqlalchemy.ext.declarative import declarative_base
import pymysql
from datetime import datetime, timezone
from fastapi import Depends

UsersBase = declarative_base()
LogsBase = declarative_base()
PredictionsBase = declarative_base()
TeamsNamesBase = declarative_base()
FIFABase = declarative_base()
MatchesResultsBase = declarative_base()

class Users(UsersBase):
    __tablename__ = "users"

    username = Column(String(100) , primary_key = True , nullable = False)
    password = Column(String(100) , nullable = False)
    is_admin = Column(Boolean , nullable = False , server_default = text("0"))
    registered_date = Column(DateTime , nullable = False)

class Logs(LogsBase):
    __tablename__ = "logs"

    username = Column(String(100) , primary_key = True , nullable = False)
    log_info = Column(String(100) , nullable = False)
    log_date = Column(DateTime , nullable = False)

class Predictions(PredictionsBase):
    __tablename__ = "predictions"

    username = Column(String(100) , primary_key = True , nullable = False)
    home_team = Column(String(100) , nullable = False)
    away_team = Column(String(100) , nullable = False)
    game_date = Column(DateTime , nullable = False)
    home_odd = Column(Float , nullable = False)
    draw_odd = Column(Float , nullable = False)
    away_odd = Column(Float , nullable = False)
    prediction_date = Column(DateTime , nullable = False)

class TeamsNames(TeamsNamesBase):
    __tablename__ = "teams_names"

    id = Column(Integer , primary_key = True , nullable = False)
    season = Column(String(100) , nullable = False)
    division = Column(String(100) , nullable = False)
    old_name = Column(String(100) , nullable = False)
    new_name = Column(String(100) , nullable = False)

class FIFA(FIFABase):
    __tablename__ = "FIFA"

    id = Column(Integer , primary_key = True , nullable = False)
    season = Column(String(100) , nullable = False)
    division = Column(String(100) , nullable = False)
    team = Column(String(100) , nullable = False)
    age = Column(Float , nullable = False)
    overall = Column(Float , nullable = False)
    potential = Column(Float , nullable = False)
    value = Column(Float , nullable = False)
    pace = Column(Float , nullable = False)
    shooting = Column(Float , nullable = False)
    passing = Column(Float , nullable = False)
    dribbling = Column(Float , nullable = False)
    defending = Column(Float , nullable = False)
    physic = Column(Float , nullable = False)

class MatchesResults(MatchesResultsBase):
    __tablename__ = "matches_results"

    ID = Column(Integer , primary_key = True , nullable = False)
    season = Column(String(100) , nullable = False)
    division = Column(String(100) , nullable = False)
    date = Column(DateTime , nullable = False)
    home_team = Column(String(100) , nullable = False)
    away_team = Column(String(100) , nullable = False)
    full_time_home_goals = Column(Integer , nullable = False)
    full_time_away_goals = Column(Integer , nullable = False)
    home_shots = Column(Integer , nullable = False)
    away_shots = Column(Integer , nullable = False)
    home_shots_on_target = Column(Integer , nullable = False)
    away_shots_on_target = Column(Integer , nullable = False)
    home_corners = Column(Integer , nullable = False)
    away_corners = Column(Integer , nullable = False)
    home_fouls = Column(Integer , nullable = False)
    away_fouls = Column(Integer , nullable = False)
    home_yellows = Column(Integer , nullable = False)
    away_yellows = Column(Integer , nullable = False)
    home_reds = Column(Integer , nullable = False)
    away_reds = Column(Integer , nullable = False)
    max_H = Column(Float , nullable = False)
    max_D = Column(Float , nullable = False)
    max_A = Column(Float , nullable = False)
    full_time_result = Column(String(1) , nullable = False)

engine = create_engine("mysql+pymysql://Mathieu:A4xpgru+@localhost/project")

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