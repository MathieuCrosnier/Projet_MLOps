from fastapi.testclient import TestClient
import os
import sys
import pytest
from datetime import datetime , timezone
from sqlalchemy.orm import sessionmaker

sys.path.append(os.path.abspath("../API"))
os.chdir(os.path.abspath("../API"))
os.environ["TEST"] = "1"

from main import api
from databases import Users , select_engine , UsersBase , add_to_users_table

client = TestClient(api)

engine = select_engine()
sm = sessionmaker(engine)
session = sm()
UsersBase.metadata.drop_all(engine)
UsersBase.metadata.create_all(bind = engine)
add_to_users_table(Users(username = "Mathieu", password = "Crosnier", is_admin = 1 , registered_date = datetime.now(timezone.utc)) , session = session)
session.close()

def test_home():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == "Welcome to SportsBetPy API !"

@pytest.mark.parametrize("params,status_code,json" , [({"username" : "Elsy" , "password" : "Barbin"} , 200 , "Your account has been created !"),
                                                      ({"username" : "Elsy" , "password" : "Crosnier"} , 401 , {'detail': "The username already exists !"}),
                                                      ({"username" : "" , "password" : ""} , 401 , {'detail': "Username or password can't be empty !"})])
def test_signup1(params , status_code , json):
    response = client.post("/signup" , params = params)
    assert response.status_code == status_code
    assert response.json() == json

@pytest.mark.parametrize("user,status_code,json" , [({"username" : "" , "password" : ""} , 422 , [{'loc': ['body', 'username'], 'msg': 'field required', 'type': 'value_error.missing'}, {'loc': ['body', 'password'], 'msg': 'field required', 'type': 'value_error.missing'}]),
                                                    ({"username" : "Micheline" , "password" : "Crobin"} , 401 , "Incorrect username or password !"),
                                                    ({"username" : "Mathieu" , "password" : "CROSNIER"} , 401 , "Incorrect username or password !"),
                                                    ({"username" : "Elsy" , "password" : "Barbin"} , 200 , None),
                                                    ({"username" : "Mathieu" , "password" : "Crosnier"} , 200 , None)])
def test_token(user , status_code , json):
    response = client.post("/token" , data = user)
    assert response.status_code == status_code
    assert response.json().get("detail") == json

def test_user():
    response = client.get("/user")
    assert response.status_code == 401
    assert response.json() == {"detail" : "Not authenticated"}

@pytest.mark.parametrize("user,status_code,json" , [({"username" : "Micheline" , "password" : "Crobin"} , 401 , {'detail': 'Your session has expired !'}),
                                                    ({"username" : "Mathieu" , "password" : "CROSNIER"} , 401 , {'detail': 'Your session has expired !'}),
                                                    ({"username" : "Elsy" , "password" : "Barbin"} , 200 , {'username': 'Elsy' , 'rights' : 'Standard'}),
                                                    ({"username" : "Mathieu" , "password" : "Crosnier"} , 200 , {'username': 'Mathieu' , 'rights' : 'Administrator'})])
def test_user2(user , status_code , json):
    token = client.post("/token" , data = user).json().get("access_token")
    response = client.get("/user" , headers = {"Authorization": f"Bearer {token}"})
    assert response.status_code == status_code
    assert response.json() == json

@pytest.mark.parametrize("user,status_code,json" , [({"username" : "Micheline" , "password" : "Crobin"} , 401 , {'detail' : 'Your session has expired !'}),
                                                    ({"username" : "Mathieu" , "password" : "CROSNIER"} , 401 , {'detail' : 'Your session has expired !'}),
                                                    ({"username" : "Elsy" , "password" : "Barbin"} , 403 , {'detail' : 'You must be an administrator to perform this action !'}),
                                                    ({"username" : "Mathieu" , "password" : "Crosnier"} , 200 , 'Tables MatchesResults and FIFA have been succesfully initialized !')])
def test_initialize_tables_matchesresults_and_fifa(user , status_code , json):
    token = client.post("/token" , data = user).json().get("access_token")
    response = client.post("/initialize_tables_matchesresults_and_fifa" , headers = {"Authorization" : f"Bearer {token}"})
    assert response.status_code == status_code
    assert response.json() == json

@pytest.mark.parametrize("user,status_code,json" , [({"username" : "Micheline" , "password" : "Crobin"} , 401 , {'detail' : 'Your session has expired !'}),
                                                    ({"username" : "Mathieu" , "password" : "CROSNIER"} , 401 , {'detail' : 'Your session has expired !'}),
                                                    ({"username" : "Elsy" , "password" : "Barbin"} , 403 , {'detail' : 'You must be an administrator to perform this action !'}),
                                                    ({"username" : "Mathieu" , "password" : "Crosnier"} , 200 , 'Table Predictions has been succesfully initialized !')])
def test_initialize_table_predictions(user , status_code , json):
    token = client.post("/token" , data = user).json().get("access_token")
    response = client.post("/initialize_table_predictions" , headers = {"Authorization" : f"Bearer {token}"})
    assert response.status_code == status_code
    assert response.json() == json

@pytest.mark.parametrize("user,status_code,json" , [({"username" : "Micheline" , "password" : "Crobin"} , 401 , {'detail' : 'Your session has expired !'}),
                                                    ({"username" : "Mathieu" , "password" : "CROSNIER"} , 401 , {'detail' : 'Your session has expired !'}),
                                                    ({"username" : "Elsy" , "password" : "Barbin"} , 403 , {'detail' : 'You must be an administrator to perform this action !'}),
                                                    ({"username" : "Mathieu" , "password" : "Crosnier"} , 200 , 'Table Users has been succesfully initialized !')])
def test_initialize_table_users(user , status_code , json):
    token = client.post("/token" , data = user).json().get("access_token")
    response = client.post("/initialize_table_users" , headers = {"Authorization" : f"Bearer {token}"})
    assert response.status_code == status_code
    assert response.json() == json

def test_signup2():
    response = client.post("/signup" , params = {"username" : "Elsy" , "password" : "Barbin"})
    assert response.status_code == 200
    assert response.json() == "Your account has been created !"

@pytest.mark.parametrize("user,status_code,json" , [({"username" : "Micheline" , "password" : "Crobin"} , 401 , {'detail' : 'Your session has expired !'}),
                                                    ({"username" : "Mathieu" , "password" : "CROSNIER"} , 401 , {'detail' : 'Your session has expired !'}),
                                                    ({"username" : "Elsy" , "password" : "Barbin"} , 403 , {'detail' : 'You must be an administrator to perform this action !'}),
                                                    ({"username" : "Mathieu" , "password" : "Crosnier"} , 200 , 'The model has been trained !')])
def test_train_model(user , status_code , json):
    token = client.post("/token" , data = user).json().get("access_token")
    response = client.post("/train_model" , headers = {"Authorization" : f"Bearer {token}"})
    assert response.status_code == status_code
    assert response.json() == json

@pytest.mark.parametrize("params,user,status_code,json" , [({"home_team" : "FC Girondins de Bordeaux" , "away_team" : "Grenoble Foot 38" , "game_date" : "2023-02-04"} , {"username" : "Micheline" , "password" : "Crobin"} , 401 , "Your session has expired !"),
                                                           ({"home_team" : "FC Girondins de Bordeaux" , "away_team" : "Grenoble Foot 38" , "game_date" : "2023-02-04"} , {"username" : "Mathieu" , "password" : "CROSNIER"} , 401 , "Your session has expired !"),
                                                           ({"home_team" : "" , "away_team" : "Grenoble Foot 38" , "game_date" : "2023-02-04"} , {"username" : "Mathieu" , "password" : "Crosnier"} , 401 , "One of the teams you selected doesn't exist !"),
                                                           ({"home_team" : "FC Girondins de Bordeaux" , "away_team" : "" , "game_date" : "2023-02-04"} , {"username" : "Mathieu" , "password" : "Crosnier"} , 401 , "One of the teams you selected doesn't exist !"),
                                                           ({"home_team" : "FC Girondins de Bordeaux" , "away_team" : "Grenoble Foot 38" , "game_date" : "2023-02-04"} , {"username" : "Elsy" , "password" : "Barbin"} , 200 , None),
                                                           ({"home_team" : "FC Girondins de Bordeaux" , "away_team" : "Grenoble Foot 38" , "game_date" : "2023-02-04"} , {"username" : "Mathieu" , "password" : "Crosnier"} , 200 , None)])
def test_prediction(params , user , status_code , json):
    token = client.post("/token" , data = user).json().get("access_token")
    response = client.post("prediction" , params = params , headers = {"Authorization": f"Bearer {token}"})
    assert response.status_code == status_code
    assert response.json().get("detail") == json


