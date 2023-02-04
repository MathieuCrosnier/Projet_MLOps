import pandas as pd
from init_databases import get_clubs_correlation_dictionary
from sklearn.neighbors import KNeighborsClassifier
from sklearn.preprocessing import StandardScaler
from fastapi import APIRouter , Depends , HTTPException , status
from joblib import dump
from access import decode_token
from databases import select_engine

router = APIRouter(tags = ["Training"])

def get_ml_df(matches_results_corrected_df : pd.DataFrame , FIFA_ratings_selected_players_df : pd.DataFrame):
    FIFA_ratings_selected_players_home_df = FIFA_ratings_selected_players_df.add_prefix("home_")
    FIFA_ratings_selected_players_home_df = FIFA_ratings_selected_players_home_df.rename(columns = {"home_season" : "season" , "home_division" : "division"})
    FIFA_ratings_selected_players_away_df = FIFA_ratings_selected_players_df.add_prefix("away_")
    FIFA_ratings_selected_players_away_df = FIFA_ratings_selected_players_away_df.rename(columns = {"away_season" : "season" , "away_division" : "division"})
       
    temp_df = matches_results_corrected_df.merge(right = FIFA_ratings_selected_players_home_df , on = ["season" , "division" , "home_team"] , how = "left")
    temp_df = temp_df.merge(right = FIFA_ratings_selected_players_away_df , on = ["season" , "division" , "away_team"] , how = "left")
    temp_df = temp_df.sort_values(by = "date")
    temp_df = temp_df.reset_index(drop = True)

    teams = temp_df["home_team"].unique()
    seasons = temp_df["season"].unique()

    for team in teams:    
        for season in seasons:
            
            temp = pd.DataFrame(dtype = "float")
            index = temp_df[(temp_df["season"] == season) & ((temp_df["home_team"] == team) | (temp_df["away_team"] == team))].index
            
            for i in index:
            
                if temp_df.loc[i , "home_team"] == team:        
                    temp.loc[i , "FTGS"] = temp_df.loc[i , "full_time_home_goals"]
                    temp.loc[i , "FTGC"] = temp_df.loc[i , "full_time_away_goals"]
                    temp.loc[i , "S"] = temp_df.loc[i , "home_shots"]
                    temp.loc[i , "ST"] = temp_df.loc[i , "home_shots_on_target"]
                
                elif temp_df.loc[i , "away_team"] == team:        
                    temp.loc[i , "FTGS"] = temp_df.loc[i , "full_time_away_goals"]
                    temp.loc[i , "FTGC"] = temp_df.loc[i , "full_time_home_goals"]
                    temp.loc[i , "S"] = temp_df.loc[i , "away_shots"]
                    temp.loc[i , "ST"] = temp_df.loc[i , "away_shots_on_target"]
                
                else:        
                    raise ValueError("There is a problem with the team name !")
            
            temp_1 = temp.rolling(1 , min_periods = 1).mean().shift(fill_value = 0)
            temp_3 = temp.rolling(3 , min_periods = 1).mean().shift(fill_value = 0)
            temp_5 = temp.rolling(5 , min_periods = 1).mean().shift(fill_value = 0)
            temp_40 = temp.rolling(40 , min_periods = 1).mean().shift(fill_value = 0)
            
            index_home = temp_df[(temp_df["season"] == season) & (temp_df["home_team"] == team)].index
            temp_1_home = temp.loc[index_home].rolling(1 , min_periods = 1).mean().shift(fill_value = 0)
            temp_3_home = temp.loc[index_home].rolling(3 , min_periods = 1).mean().shift(fill_value = 0)
            temp_5_home = temp.loc[index_home].rolling(5 , min_periods = 1).mean().shift(fill_value = 0)
            temp_20_home = temp.loc[index_home].rolling(20 , min_periods = 1).mean().shift(fill_value = 0)

            index_away = temp_df[(temp_df["season"] == season) & (temp_df["away_team"] == team)].index
            temp_1_away = temp.loc[index_away].rolling(1 , min_periods = 1).mean().shift(fill_value = 0)
            temp_3_away = temp.loc[index_away].rolling(3 , min_periods = 1).mean().shift(fill_value = 0)
            temp_5_away = temp.loc[index_away].rolling(5 , min_periods = 1).mean().shift(fill_value = 0)
            temp_20_away = temp.loc[index_away].rolling(20 , min_periods = 1).mean().shift(fill_value = 0)

            for i in index:
            
                if temp_df.loc[i , "home_team"] == team:        
                    
                    temp_df.loc[i , "home_Full time goals scored (1 game)"] = temp_1.loc[i , "FTGS"]
                    temp_df.loc[i , "home_Full time goals conceded (1 game)"] = temp_1.loc[i , "FTGC"]
                    temp_df.loc[i , "home_Shots (1 game)"] = temp_1.loc[i , "S"]
                    temp_df.loc[i , "home_Shots on target (1 game)"] = temp_1.loc[i , "ST"]
                    
                    temp_df.loc[i , "home_Full time goals scored (home or away) (1 game)"] = temp_1_home.loc[i , "FTGS"]
                    temp_df.loc[i , "home_Full time goals conceded (home or away) (1 game)"] = temp_1_home.loc[i , "FTGC"]
                    temp_df.loc[i , "home_Shots (home or away) (1 game)"] = temp_1_home.loc[i , "S"]
                    temp_df.loc[i , "home_Shots on target (home or away) (1 game)"] = temp_1_home.loc[i , "ST"]

                    temp_df.loc[i , "home_Full time goals scored (3 games)"] = temp_3.loc[i , "FTGS"]
                    temp_df.loc[i , "home_Full time goals conceded (3 games)"] = temp_3.loc[i , "FTGC"]
                    temp_df.loc[i , "home_Shots (3 games)"] = temp_3.loc[i , "S"]
                    temp_df.loc[i , "home_Shots on target (3 games)"] = temp_3.loc[i , "ST"]
                    
                    temp_df.loc[i , "home_Full time goals scored (home or away) (3 games)"] = temp_3_home.loc[i , "FTGS"]
                    temp_df.loc[i , "home_Full time goals conceded (home or away) (3 games)"] = temp_3_home.loc[i , "FTGC"]
                    temp_df.loc[i , "home_Shots (home or away) (3 games)"] = temp_3_home.loc[i , "S"]
                    temp_df.loc[i , "home_Shots on target (home or away) (3 games)"] = temp_3_home.loc[i , "ST"]

                    temp_df.loc[i , "home_Full time goals scored (5 games)"] = temp_5.loc[i , "FTGS"]
                    temp_df.loc[i , "home_Full time goals conceded (5 games)"] = temp_5.loc[i , "FTGC"]
                    temp_df.loc[i , "home_Shots (5 games)"] = temp_5.loc[i , "S"]
                    temp_df.loc[i , "home_Shots on target (5 games)"] = temp_5.loc[i , "ST"]
                    
                    temp_df.loc[i , "home_Full time goals scored (home or away) (5 games)"] = temp_5_home.loc[i , "FTGS"]
                    temp_df.loc[i , "home_Full time goals conceded (home or away) (5 games)"] = temp_5_home.loc[i , "FTGC"]
                    temp_df.loc[i , "home_Shots (home or away) (5 games)"] = temp_5_home.loc[i , "S"]
                    temp_df.loc[i , "home_Shots on target (home or away) (5 games)"] = temp_5_home.loc[i , "ST"]

                    temp_df.loc[i , "home_Full time goals scored (home or away) (20 games)"] = temp_20_home.loc[i , "FTGS"]
                    temp_df.loc[i , "home_Full time goals conceded (home or away) (20 games)"] = temp_20_home.loc[i , "FTGC"]
                    temp_df.loc[i , "home_Shots (home or away) (20 games)"] = temp_20_home.loc[i , "S"]
                    temp_df.loc[i , "home_Shots on target (home or away) (20 games)"] = temp_20_home.loc[i , "ST"]
                    
                    temp_df.loc[i , "home_Full time goals scored (40 games)"] = temp_40.loc[i , "FTGS"]
                    temp_df.loc[i , "home_Full time goals conceded (40 games)"] = temp_40.loc[i , "FTGC"]
                    temp_df.loc[i , "home_Shots (40 games)"] = temp_40.loc[i , "S"]
                    temp_df.loc[i , "home_Shots on target (40 games)"] = temp_40.loc[i , "ST"]
                
                elif temp_df.loc[i , "away_team"] == team:       
                    
                    temp_df.loc[i , "away_Full time goals scored (1 game)"] = temp_1.loc[i , "FTGS"]
                    temp_df.loc[i , "away_Full time goals conceded (1 game)"] = temp_1.loc[i , "FTGC"]
                    temp_df.loc[i , "away_Shots (1 game)"] = temp_1.loc[i , "S"]
                    temp_df.loc[i , "away_Shots on target (1 game)"] = temp_1.loc[i , "ST"]

                    temp_df.loc[i , "away_Full time goals scored (home or away) (1 game)"] = temp_1_away.loc[i , "FTGS"]
                    temp_df.loc[i , "away_Full time goals conceded (home or away) (1 game)"] = temp_1_away.loc[i , "FTGC"]
                    temp_df.loc[i , "away_Shots (home or away) (1 game)"] = temp_1_away.loc[i , "S"]
                    temp_df.loc[i , "away_Shots on target (home or away) (1 game)"] = temp_1_away.loc[i , "ST"]

                    temp_df.loc[i , "away_Full time goals scored (3 games)"] = temp_3.loc[i , "FTGS"]
                    temp_df.loc[i , "away_Full time goals conceded (3 games)"] = temp_3.loc[i , "FTGC"]
                    temp_df.loc[i , "away_Shots (3 games)"] = temp_3.loc[i , "S"]
                    temp_df.loc[i , "away_Shots on target (3 games)"] = temp_3.loc[i , "ST"]

                    temp_df.loc[i , "away_Full time goals scored (home or away) (3 games)"] = temp_3_away.loc[i , "FTGS"]
                    temp_df.loc[i , "away_Full time goals conceded (home or away) (3 games)"] = temp_3_away.loc[i , "FTGC"]
                    temp_df.loc[i , "away_Shots (home or away) (3 games)"] = temp_3_away.loc[i , "S"]
                    temp_df.loc[i , "away_Shots on target (home or away) (3 games)"] = temp_3_away.loc[i , "ST"]
                    
                    temp_df.loc[i , "away_Full time goals scored (5 games)"] = temp_5.loc[i , "FTGS"]
                    temp_df.loc[i , "away_Full time goals conceded (5 games)"] = temp_5.loc[i , "FTGC"]
                    temp_df.loc[i , "away_Shots (5 games)"] = temp_5.loc[i , "S"]
                    temp_df.loc[i , "away_Shots on target (5 games)"] = temp_5.loc[i , "ST"]

                    temp_df.loc[i , "away_Full time goals scored (home or away) (5 games)"] = temp_5_away.loc[i , "FTGS"]
                    temp_df.loc[i , "away_Full time goals conceded (home or away) (5 games)"] = temp_5_away.loc[i , "FTGC"]
                    temp_df.loc[i , "away_Shots (home or away) (5 games)"] = temp_5_away.loc[i , "S"]
                    temp_df.loc[i , "away_Shots on target (home or away) (5 games)"] = temp_5_away.loc[i , "ST"]

                    temp_df.loc[i , "away_Full time goals scored (home or away) (20 games)"] = temp_20_away.loc[i , "FTGS"]
                    temp_df.loc[i , "away_Full time goals conceded (home or away) (20 games)"] = temp_20_away.loc[i , "FTGC"]
                    temp_df.loc[i , "away_Shots (home or away) (20 games)"] = temp_20_away.loc[i , "S"]
                    temp_df.loc[i , "away_Shots on target (home or away) (20 games)"] = temp_20_away.loc[i , "ST"]
                    
                    temp_df.loc[i , "away_Full time goals scored (40 games)"] = temp_40.loc[i , "FTGS"]
                    temp_df.loc[i , "away_Full time goals conceded (40 games)"] = temp_40.loc[i , "FTGC"]
                    temp_df.loc[i , "away_Shots (40 games)"] = temp_40.loc[i , "S"]
                    temp_df.loc[i , "away_Shots on target (40 games)"] = temp_40.loc[i , "ST"]
                
                else:
                    raise ValueError("There is a problem with the team name !")

    teams = temp_df["home_team"].unique()
    seasons = temp_df["season"].unique()

    for team in teams:    
        for season in seasons:
            
            temp = pd.Series(dtype = "float")
            index = temp_df[(temp_df["season"] == season) & ((temp_df["home_team"] == team) | (temp_df["away_team"] == team))].index
            
            for i in index:
            
                if temp_df.loc[i , "home_team"] == team:
                    if temp_df.loc[i , "full_time_result"] == "H":       
                        temp.loc[i] = 3
                    elif temp_df.loc[i , "full_time_result"] == "D":
                        temp.loc[i] = 1
                    elif temp_df.loc[i , "full_time_result"] == "A":
                        temp.loc[i] = 0
                    else:
                        raise ValueError("'full_time_result' takes an incorrect value !")
                
                elif temp_df.loc[i , "away_team"] == team:        
                    if temp_df.loc[i , "full_time_result"] == "A":       
                        temp.loc[i] = 3
                    elif temp_df.loc[i , "full_time_result"] == "D":
                        temp.loc[i] = 1
                    elif temp_df.loc[i , "full_time_result"] == "H":
                        temp.loc[i] = 0
                    else:
                        raise ValueError("'full_time_result' takes an incorrect value !")
                
                else:        
                    raise ValueError("There is a problem with the name of the team !")
            
            temp_1 = temp.rolling(1 , min_periods = 1).mean().shift(fill_value = 0)
            temp_3 = temp.rolling(3 , min_periods = 1).mean().shift(fill_value = 0)
            temp_5 = temp.rolling(5 , min_periods = 1).mean().shift(fill_value = 0)
            temp_40 = temp.rolling(40 , min_periods = 1).mean().shift(fill_value = 0)
            
            index_home = temp_df[(temp_df["season"] == season) & (temp_df["home_team"] == team)].index
            temp_1_home = temp.loc[index_home].rolling(1 , min_periods = 1).mean().shift(fill_value = 0)
            temp_3_home = temp.loc[index_home].rolling(3 , min_periods = 1).mean().shift(fill_value = 0)
            temp_5_home = temp.loc[index_home].rolling(5 , min_periods = 1).mean().shift(fill_value = 0)
            temp_20_home = temp.loc[index_home].rolling(20 , min_periods = 1).mean().shift(fill_value = 0)

            index_away = temp_df[(temp_df["season"] == season) & (temp_df["away_team"] == team)].index
            temp_1_away = temp.loc[index_away].rolling(1 , min_periods = 1).mean().shift(fill_value = 0)
            temp_3_away = temp.loc[index_away].rolling(3 , min_periods = 1).mean().shift(fill_value = 0)
            temp_5_away = temp.loc[index_away].rolling(5 , min_periods = 1).mean().shift(fill_value = 0)
            temp_20_away = temp.loc[index_away].rolling(20 , min_periods = 1).mean().shift(fill_value = 0)
            
            for i in index:
            
                if temp_df.loc[i , "home_team"] == team:        
                    temp_df.loc[i , "home_Points (1 game)"] = temp_1.loc[i]
                    temp_df.loc[i , "home_Points (3 games)"] = temp_3.loc[i]
                    temp_df.loc[i , "home_Points (5 games)"] = temp_5.loc[i]
                    temp_df.loc[i , "home_Points (40 games)"] = temp_40.loc[i]

                    temp_df.loc[i , "home_Points (home or away) (1 game)"] = temp_1_home.loc[i]
                    temp_df.loc[i , "home_Points (home or away) (3 games)"] = temp_3_home.loc[i]
                    temp_df.loc[i , "home_Points (home or away) (5 games)"] = temp_5_home.loc[i]
                    temp_df.loc[i , "home_Points (home or away) (20 games)"] = temp_20_home.loc[i]

                elif temp_df.loc[i , "away_team"] == team:       
                    temp_df.loc[i , "away_Points (1 game)"] = temp_1.loc[i]
                    temp_df.loc[i , "away_Points (3 games)"] = temp_3.loc[i]
                    temp_df.loc[i , "away_Points (5 games)"] = temp_5.loc[i]
                    temp_df.loc[i , "away_Points (40 games)"] = temp_40.loc[i]

                    temp_df.loc[i , "away_Points (home or away) (1 game)"] = temp_1_away.loc[i]
                    temp_df.loc[i , "away_Points (home or away) (3 games)"] = temp_3_away.loc[i]
                    temp_df.loc[i , "away_Points (home or away) (5 games)"] = temp_5_away.loc[i]
                    temp_df.loc[i , "away_Points (home or away) (20 games)"] = temp_20_away.loc[i]

                else:
                    raise ValueError("There is a problem with the team name !")

    temp_df = temp_df.reset_index(drop = True)
    temp_df.to_csv("output_data/temp_df.csv" , index = False)
    df_matches_results_columns = ["season" , "division" , "date" , "home_team" , "away_team" , "full_time_home_goals" , "full_time_away_goals" , "home_shots" , "away_shots" , "home_shots_on_target" , "away_shots_on_target" , "home_fouls" , "away_fouls" , "home_corners" , "away_corners" , "home_yellows" , "away_yellows" , "home_reds" , "away_reds" , "max_H" , "max_D" , "max_A" , "full_time_result"]
    df_matches_results = temp_df[df_matches_results_columns]
    temp_df = temp_df.drop(columns = df_matches_results.columns)
    df_home = temp_df.loc[: , temp_df.columns[temp_df.columns.str.contains("home_")]]
    df_away = temp_df.loc[: , temp_df.columns[temp_df.columns.str.contains("away_")]]
    df_home.columns = [x.replace("home_" , "") for x in df_home.columns]
    df_away.columns = [x.replace("away_" , "") for x in df_away.columns]
    df = df_matches_results.join(df_home - df_away)
    df["Cote"] = df["max_A"] - df["max_H"]
    df = df.reset_index(drop = True)

    df.to_csv("output_data/complete_dataset.csv" , index = False)

    df = df.drop(columns = df_matches_results.columns.drop("full_time_result"))
    df = df.reset_index(drop = True)
    print(f"\nLe dataframe comporte {df.shape[0]} matches.")
    print(f"\nLe dataframe comporte {df.isna().sum().sum()} valeurs manquantes.")
    df.to_csv("output_data/final_dataset.csv" , index = False)

    return df

def train_model(ml_df : pd.DataFrame):
    X_train = ml_df.drop(columns = "full_time_result")
    y_train = ml_df["full_time_result"]
    scaler = StandardScaler().fit(X_train)
    X_train_scaled = pd.DataFrame(scaler.transform(X_train) , index = X_train.index , columns = X_train.columns)
    X_train.to_csv("output_data/X_train.csv")
    X_train_scaled.to_csv("output_data/X_train_scaled.csv")
    model = KNeighborsClassifier(n_neighbors = 134)
    model.fit(X_train_scaled , y_train)
    dump(model , "output_data/model.pkl")
    dump(scaler , "output_data/scaler.pkl")
    
    return

@router.post("/train_model" , name = "Train the Machine Learning model")
async def get_trained_model(user = Depends(decode_token) , engine = Depends(select_engine)):
    if user.get("rights") != "Administrator":
        raise HTTPException(
            status_code = status.HTTP_403_FORBIDDEN ,
            detail = "You must be an administrator to perform this action !" ,
            headers = {"WWW-Authenticate": "Bearer"})
    matches_results_corrected_df = pd.read_sql(sql = "SELECT * FROM matches_results", con = engine).drop(columns = "id")
    FIFA_ratings_selected_players_df = pd.read_sql(sql = "SELECT * FROM FIFA", con = engine).drop(columns = "id")
    ml_df = get_ml_df(matches_results_corrected_df = matches_results_corrected_df , FIFA_ratings_selected_players_df = FIFA_ratings_selected_players_df)
    train_model(ml_df)

    return "The model has been trained !"