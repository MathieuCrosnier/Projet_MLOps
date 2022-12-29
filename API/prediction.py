import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from fastapi import APIRouter
from pydantic import BaseModel
import joblib

df = pd.read_csv("games.csv" , index_col = 0)

df_to_bet = df[df["Season"] == "2020-2021"]

X = df.drop(columns = ["Season" , "Division" , "Date" , "Home team" , "Away team" , "FTHG" , "FTAG" , "FTR" , "Max H" , "Max D" , "Max A"])
y = df["FTR"]
    
X_train = X[df["Season"] != "2020-2021"]
X_test = X[df["Season"] == "2020-2021"]
y_train = y[df["Season"] != "2020-2021"]
y_test = y[df["Season"] == "2020-2021"]

scaler = StandardScaler().fit(X_train)
X_train_scaled = pd.DataFrame(scaler.transform(X_train) , index = X_train.index , columns = X_train.columns) 
X_test_scaled = pd.DataFrame(scaler.transform(X_test) , index = X_test.index , columns = X_test.columns)

model = joblib.load("model.pkl")

router = APIRouter(tags = ["Prediction"])

@router.get("/prediction" , name = "Get model prediction")
async def prediction(home_team : str , away_team : str , date : str):
    game = X_test_scaled[(df_to_bet["Home team"] == home_team) & (df_to_bet["Away team"] == away_team) & (df_to_bet["Date"] == date)]
    probs = model.predict_proba(game)[0]
    odds = np.round(1 / probs , 2)
    
    return {
        "Cote victoire {}".format(home_team) : odds[2] ,
         "Cote match nul" : odds[1] ,
          "Cote victoire {}".format(away_team) : odds[0]
    }