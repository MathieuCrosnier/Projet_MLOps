from fastapi import FastAPI
import access

api = FastAPI(title = "API SportsBetPy")
api.include_router(access.router)

@api.get("/" , name = "Welcome message")
async def welcome_message():
    return "Bienvenue sur l'API de SportsBetPy"