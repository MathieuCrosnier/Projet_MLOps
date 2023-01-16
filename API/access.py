from fastapi import APIRouter , Depends , HTTPException , status
from fastapi.security import OAuth2PasswordBearer , OAuth2PasswordRequestForm
from jwt import encode , decode , PyJWTError
from datetime import datetime , timezone , timedelta
from sqlalchemy.orm import Session
from databases import start_session , current_user , add_to_users_table , Users

router = APIRouter(tags = ["Access"])

oauth2 = OAuth2PasswordBearer(tokenUrl = "token")
secret_key = "9290a6c64b338dcb7cc17afe83310e284e976670d07580799b35f86ba0bab74a"
algorithm = "HS256"
token_expiration = 5

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
                            detail = "Your session has expired.",
                            headers = {"WWW-Authenticate": "Bearer"})
    return {"username" : username , "rights" : rights}

@router.post("/token" , name = "Generate token")
async def token(credentials : OAuth2PasswordRequestForm = Depends() , session = Depends(start_session)):
    username = credentials.username
    password = credentials.password
    user = current_user(username = username , session = session)
    if (user is None) or ((user is not None) & (password != user.password)):
        raise HTTPException(
            status_code = status.HTTP_401_UNAUTHORIZED ,
            detail = "Incorrect username or password" ,
            headers = {"WWW-Authenticate": "Bearer"} ,
        )    
    token = generate_token(user)
    return {"access_token": token, "token_type": "bearer"}

@router.post("/signup" , name = "Create an account")
async def signup(username : str , password : str , session : Session = Depends(start_session)):
    if (username is None) or (password is None):
        raise HTTPException(
            status_code = status.HTTP_401_UNAUTHORIZED ,
            detail = "Username or password can't be empty"
        )
    user = current_user(username = username , session = session)
    if user is not None:
        raise HTTPException(
            status_code = status.HTTP_401_UNAUTHORIZED ,
            detail = "The username already exists"
        )
    if user is None:
        user = Users(username = username , password = password , registered_date = datetime.now(timezone.utc))
        add_to_users_table(user = user , session = session)
    return "Your account has been created"

@router.get("/user" , name = "Get user information")
async def user(token : str = Depends(oauth2)):
    return decode_token(token)