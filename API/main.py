from fastapi import FastAPI
import access
import prediction
import init_databases
import train_model
from databases import select_engine
from init_databases import create_tables_matchesresults_and_fifa , create_table_users , create_table_predictions
from update_databases import update_matches_results_table
from sqlalchemy.orm import sessionmaker

api = FastAPI(title = "API SportsBetPy")
api.include_router(access.router)
api.include_router(prediction.router)
api.include_router(init_databases.router)
api.include_router(train_model.router)

async def create_tables_on_startup():
    print("CREATION OF TABLES")
    engine = select_engine()
    sm = sessionmaker(engine)
    session = sm()   
    create_tables_matchesresults_and_fifa(session = session)    
    create_table_users(session = session)
    create_table_predictions(session = session)
    session.close()

async def update_matches_results_database_on_startup():
    print("UPDATE OF TABLES")
    update_matches_results_table(seasons = ["2022-2023"] , FIFA_files = ["FIFA23.csv"])

@api.on_event("startup")
async def startup():
    await create_tables_on_startup()
    await update_matches_results_database_on_startup()

@api.get("/" , name = "Welcome message" , tags = ["Home page"])
async def welcome_message():
    return "Welcome to SportsBetPy API !"
