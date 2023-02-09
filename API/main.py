from fastapi import FastAPI , Depends
import access
import prediction
import init_databases
import train_model
from databases import create_tables , select_engine
from sqlalchemy.orm import sessionmaker

api = FastAPI(title = "API SportsBetPy")

@api.on_event("startup")
async def create_tables_on_startup():
    engine = select_engine()
    sm = sessionmaker(engine)
    session = sm()   
    create_tables(session = session)
    session.close()

@api.get("/" , name = "Welcome message" , tags = ["Home page"])
async def welcome_message():
    return "Bienvenue sur l'API de SportsBetPy"

api.include_router(access.router)
api.include_router(prediction.router)
api.include_router(init_databases.router)
api.include_router(train_model.router)