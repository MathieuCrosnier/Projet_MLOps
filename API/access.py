from fastapi import APIRouter , Depends , HTTPException , status
from fastapi.security import OAuth2PasswordBearer , OAuth2PasswordRequestForm
from jwt import encode , decode
from typing import Optional
from pydantic import BaseModel

router = APIRouter(tags = ["Access"])

class User(BaseModel):
    '''
	Format pour la gestion des utilisateurs.
	'''
    username : str
    password : str
    is_admin : bool

users = []

admin = User(username = "Mathieu" , password = "Crosnier" , is_admin = True)

oauth2 = OAuth2PasswordBearer(tokenUrl = "token")
secret_key = "9290a6c64b338dcb7cc17afe83310e284e976670d07580799b35f86ba0bab74a"
algorithm = "HS256"

def existing_user(username : str):
    result = list(filter(lambda x : x.username == username , users))
    if result == []:
        return None
    else:
        return result[0]

def is_existing_user(username : str):
    result = existing_user(username) 
    if result is None:
        return False
    else:
        return True

def save_user(user : User):
    users.append(user)

save_user(admin)

def generate_token(user : User):
    dic = {"username" : user.username ,
           "rights" : "Administrator" if user.is_admin else "Standard"}
    encoded_jwt = encode(dic , secret_key , algorithm = algorithm)
    return encoded_jwt

def decode_token(token : str):
    payload = decode(token , secret_key , algorithms = [algorithm])
    username : str = payload.get("username")
    rights : str = payload.get("rights")
    return {"username" : username , "rights" : rights}

@router.post("/token" , name = "Generate token")
async def token(credentials : OAuth2PasswordRequestForm = Depends()):
    username = credentials.username
    password = credentials.password
    user = existing_user(username)
    if (not is_existing_user(username)) or (is_existing_user(username) & (password != user.password)):
        raise HTTPException(
            status_code = status.HTTP_401_UNAUTHORIZED ,
            detail = "Incorrect username or password" ,
            headers = {"WWW-Authenticate": "Bearer"} ,
        )    
    token = generate_token(user)
    return {"access_token": token, "token_type": "bearer"}

@router.post("/signup" , name = "Create an account")
async def signup(username : str , password : str):
    if (username is None) or (password is None):
        raise HTTPException(
            status_code = status.HTTP_401_UNAUTHORIZED ,
            detail = "Username or password can't be empty"
        )
    if not existing_user(username):
        user = User(username = username , password = password , is_admin = False)
        users.append(user)
    return "Your account has been created"

@router.get("/user" , name = "Get user information")
async def user(token : str = Depends(oauth2)):
    return decode_token(token)

@router.get("/users" , name = "Return list of users")
async def get_users():
    return users