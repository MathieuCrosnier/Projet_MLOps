from fastapi import APIRouter , Depends , HTTPException , status , Query
from fastapi.security import OAuth2PasswordBearer , OAuth2PasswordRequestForm
from jwt import encode , decode , PyJWTError
from datetime import datetime , timezone , timedelta
from sqlalchemy.orm import Session
from databases import start_session , current_user , add_to_users_table , Users
import yaml

router = APIRouter(tags = ["Access"])

oauth2 = OAuth2PasswordBearer(tokenUrl = "token")

with open("parameters.yml", "r") as stream:
    parameters = yaml.safe_load(stream)

secret_key = parameters.get("jwt").get("SECRET_KEY")
algorithm = parameters.get("jwt").get("ALGORITHM")
token_expiration = parameters.get("jwt").get("TOKEN_EXPIRATION")

def generate_token(user : Users):
    dic = {"username" : user.username ,
           "rights" : "Administrator" if user.is_admin else "Standard" ,
           "exp" : datetime.now(timezone.utc) + timedelta(minutes = token_expiration)}
    encoded_jwt = encode(dic , secret_key , algorithm = algorithm)
    return encoded_jwt

def decode_token(token : str = Depends(oauth2)):
    try:
        payload = decode(token , secret_key , algorithms = [algorithm])
        username : str = payload.get("username")
        rights : str = payload.get("rights")
    except PyJWTError:
        raise HTTPException(status_code = status.HTTP_401_UNAUTHORIZED,
                            detail = "Your session has expired !",
                            headers = {"WWW-Authenticate": "Bearer"})
    
    return {"username" : username , "rights" : rights}

@router.post("/token" , name = "Generate token" , responses = {401 : {"description" : "Incorrect username or password !"}})
async def token(credentials : OAuth2PasswordRequestForm = Depends() , session = Depends(start_session)):
    """
    Generates a JWT token.
    """
    username = credentials.username
    password = credentials.password
    user = current_user(username = username , session = session)
    if (user is None) or ((user is not None) & (password != user.password)):
        raise HTTPException(
            status_code = status.HTTP_401_UNAUTHORIZED ,
            detail = "Incorrect username or password !"
        )    
    token = generate_token(user)
    return {"access_token": token, "token_type": "bearer"}

@router.post("/signup" , name = "Create an account" , responses = {400 : {"description" : "The username already exists !"}})
async def signup(username : str = Query(alias = "Username") , password : str = Query(alias = "Password") , session : Session = Depends(start_session)):
    """
    Creates an account.  
    Your account will be created with standard rights.
    """
    user = current_user(username = username , session = session)
    if user is not None:
        raise HTTPException(
            status_code = status.HTTP_400_BAD_REQUEST ,
            detail = "The username already exists !"
        )
    if user is None:
        user = Users(username = username , password = password , registered_date = datetime.now(timezone.utc))
        add_to_users_table(user = user , session = session)
    return "Your account has been created !"

@router.get("/user" , name = "Get user information" , responses = {401 : {"description" : "You must sign in first !"}})
async def user(token : str = Depends(oauth2)):
    """
    Returns user's username and rights.
    """
    return decode_token(token)