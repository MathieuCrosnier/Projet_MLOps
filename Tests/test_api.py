from fastapi.testclient import TestClient
import os
import sys
import pytest
from datetime import datetime , timezone
from sqlalchemy.orm import sessionmaker

sys.path.append(os.path.abspath("../API"))
os.chdir(os.path.abspath("../API"))

from main import api
from access import generate_token
from databases import Users , select_engine , UsersBase , add_to_users_table , start_session

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
    assert response.json() == "Bienvenue sur l'API de SportsBetPy"

def test_user():
    response = client.get("/user")
    assert response.status_code == 401
    assert response.json() == {"detail" : "Not authenticated"}

@pytest.mark.parametrize("user,status_code,json" , [(Users(username = "Mathieu" , is_admin = True) , 200 , {"username" : "Mathieu" , "rights" : "Administrator"}),
                                                    (Users(username = "Micheline" , is_admin = False) , 200 , {"username" : "Micheline" , "rights" : "Standard"})])
def test_user2(user , status_code , json):
    token = generate_token(user)
    response = client.get("/user" , headers = {"Authorization": f"Bearer {token}"})
    assert response.status_code == status_code
    assert response.json() == json

def test_token1():
    response = client.post("/token" , data = {"username" : "Micheline" , "password" : "Barbin"})
    assert response.status_code == 401
    assert response.json() == {'detail': 'Incorrect username or password'}

def test_token2():
    response = client.post("/token" , data = {"username" : "Mathieu" , "password" : "Crosnier"})
    assert response.status_code == 200

@pytest.mark.parametrize("params,status_code,json" , [({"username" : "Elsy" , "password" : "Barbin"} , 200 , "Your account has been created"),
                                                      ({"username" : "Elsy" , "password" : "Barbin"} , 401 , {'detail': "The username already exists"}),
                                                      ({"username" : "" , "password" : ""} , 401 , {'detail': "Username or password can't be empty"})])
def test_signup(params , status_code , json):
    response = client.post("/signup" , params = params)
    assert response.status_code == status_code
    assert response.json() == json

@pytest.mark.parametrize("user,status_code,json" , [(Users(username = "Elsy" , is_admin = False) , 403 , {"detail": "You must be an administrator to perform this action."}),
                                                    (Users(username = "Mathieu" , is_admin = True) , 200 , "Les bases de données ont été réinitialisées avec succès !")])
def test_initialize_databases(user , status_code , json):
    token = generate_token(user)
    response = client.post("/initialize_databases" , headers = {"Authorization" : f"Bearer {token}"})
    assert response.status_code == status_code
    assert response.json() == json