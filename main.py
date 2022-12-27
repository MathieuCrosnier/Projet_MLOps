from fastapi import FastAPI , Header , HTTPException , Depends , status
from fastapi.security import OAuth2PasswordBearer , OAuth2PasswordRequestForm
from joblib import load
from pydantic import BaseModel
from jwt import encode , decode
from datetime import datetime, timedelta

api = FastAPI(title = "API SportsBetPy")

users = {
    "Mathieu" : {
        "password" : "Crosnier" ,
        "is_admin" : True
        }
}

oauth2 = OAuth2PasswordBearer(tokenUrl = "token")
secret_key = "9290a6c64b338dcb7cc17afe83310e284e976670d07580799b35f86ba0bab74a"
algorithm = "HS256"

def generate_token(username : str , is_admin : bool):
    dic = {"username" : username ,
           "rights" : "Administrator" if is_admin else "Standard"}
    encoded_jwt = encode(dic , secret_key , algorithm = algorithm)
    return encoded_jwt

def decode_token(token : str):
    payload = decode(token , secret_key , algorithms = [algorithm])
    username : str = payload.get("username")
    rights : str = payload.get("rights")
    return {"username" : username , "rights" : rights}

@api.post("/token")
async def token(credentials : OAuth2PasswordRequestForm = Depends()):
    username = credentials.username
    password = credentials.password
    if (username not in users) or ((username in users) & (password != users.get(username).get("password"))):
        raise HTTPException(
            status_code = status.HTTP_401_UNAUTHORIZED ,
            detail = "Incorrect username or password" ,
            headers = {"WWW-Authenticate": "Bearer"} ,
        )    
    token = generate_token(username = username , is_admin = users.get(username).get("is_admin"))
    users.get(username)["token"] = token
    return {"access_token": token, "token_type": "bearer"}

@api.get("/user")
async def user(token : str = Depends(oauth2)):
    return decode_token(token)