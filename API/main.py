from fastapi import FastAPI
import access
import prediction

api = FastAPI(title = "API SportsBetPy")
api.include_router(access.router)
api.include_router(prediction.router)

@api.get("/" , name = "Welcome message")
async def welcome_message():
    return "Bienvenue sur l'API de SportsBetPy"