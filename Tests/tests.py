from fastapi.testclient import TestClient
import os
import sys

sys.path.append(os.path.abspath("../API"))
os.chdir(os.path.abspath("../API"))
print(os.getcwd())

from main import api
from access import generate_token
from databases import Users

client = TestClient(api)

def test_home():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == "Bienvenue sur l'API de SportsBetPy"

def test_user1():
    response = client.get("/user")
    assert response.status_code == 401
    assert response.json() == {"detail" : "Not authenticated"}

def test_user2():
    user = Users(username = "Mathieu" , is_admin = False)
    token = generate_token(user)
    response = client.get("/user" , headers = {"Authorization": "Bearer {}".format(token)})
    assert response.status_code == 200
    assert response.json() == {"username" : "Mathieu" , "rights" : "Standard"}

def test_token1():
    response = client.post("/token" , data = {"username" : "Micheline" , "password" : "Barbin"})
    assert response.status_code == 401
    assert response.json() == {'detail': 'Incorrect username or password'}

def test_token2():
    response = client.post("/token" , data = {"username" : "Mathieu" , "password" : "Crosnier"})
    assert response.status_code == 200

def test_signup():
    response = client.post("/signup" , params = {"username" : "Elsy" , "password" : "Barbin"})
    assert response.status_code == 200
    assert response.json() == "Your account has been created"