import pandas as pd
import numpy as np
from os import listdir
import re
from fastapi import APIRouter , Depends , HTTPException , status
from access import decode_token
from databases import MatchesResultsBase , FIFABase , UsersBase , PredictionsBase , Users , add_to_users_table , reset_tables_matchesresults_and_fifa , reset_table_users , reset_table_predictions , start_session , select_engine , select_output_data_folder
from sqlalchemy.orm import Session
from sqlalchemy import inspect
from datetime import datetime , timezone

router = APIRouter(tags = ["Databases"])

def get_matches_results_df(seasons : list):
    input_data_address = "input_data/matches_results/"
    matches_results_df = pd.DataFrame()
    print("\nLa liste des fichiers traités est la suivante :\n")
    for folder in seasons:

        for file in listdir(input_data_address + "/" + folder):
            print(folder + "/" + file)
            df = pd.read_csv(input_data_address + folder + "/" + file , encoding = 'unicode_escape')
            df.insert(0 , "season" , folder)
    
            if "HFKC" in list(df.columns):
                df = df.rename(columns = {"HFKC" : "HF" , "AFKC" : "AF"})
    
            df["Div"] = file.strip(".csv").replace("_" , " ")
            matches_results_df = pd.concat([matches_results_df , df])
    
    matches_results_df = matches_results_df.reset_index(drop = True)
    matches_statistics = ["season" , "Div" , "Date" , "HomeTeam" , "AwayTeam" , "FTHG" , "FTAG" , "FTR" , "HS" , "AS" , "HST" , "AST" , "HC" , "AC" , "HF" , "AF" , "HY" , "AY" , "HR" , "AR"]
    bookmakers_list = ["B365H" , "B365D" , "B365A" , "BWH" , "BWD" , "BWA" , "IWH" , "IWD" , "IWA" , "LBH" , "LBD" , "LBA" , "PSH" , "PSD" , "PSA" , "SJH" , "SJD" , "SJA" , "VCH" , "VCD" , "VCA" , "WHH" , "WHD" , "WHA"]
    bookmakers_closing_list = ["B365CH" , "B365CD" , "B365CA" , "BWCH" , "BWCD" , "BWCA" , "IWCH" , "IWCD" , "IWCA" , "PSCH" , "PSCD" , "PSCA" , "VCCH" , "VCCD" , "VCCA" , "WHCH" , "WHCD" , "WHCA"]
    bookmakers_total_list = bookmakers_list + bookmakers_closing_list
    matches_results_df_columns = matches_results_df.columns[matches_results_df.columns.isin(matches_statistics + bookmakers_total_list)]
    matches_results_df_bookmakers_columns = matches_results_df.columns[matches_results_df.columns.isin(bookmakers_total_list)]
    matches_results_df = matches_results_df[matches_results_df_columns]
    matches_results_df = matches_results_df.drop(matches_results_df[matches_results_df[matches_statistics].isna().any(axis = 1)].index)
    matches_results_df = matches_results_df.drop(matches_results_df[matches_results_df[matches_results_df_bookmakers_columns].isna().all(axis = 1)].index)

    bookmakers_H = []
    bookmakers_D = []
    bookmakers_A = []

    rH = re.compile(r"\w+H$")
    rD = re.compile(r"\w+D$")
    rA = re.compile(r"\w+A$")

    for col in matches_results_df_bookmakers_columns:
    
        if rH.findall(col) != []:
            bookmakers_H.append(rH.findall(col)[0])
            
        elif rD.findall(col) != []:
            bookmakers_D.append(rD.findall(col)[0])
        
        elif rA.findall(col) != []:
            bookmakers_A.append(rA.findall(col)[0])
        
        else:
            raise ValueError("Some odds columns are missing !")

    matches_results_df["Max H"] = matches_results_df[bookmakers_H].max(axis = 1)
    matches_results_df["Max D"] = matches_results_df[bookmakers_D].max(axis = 1)
    matches_results_df["Max A"] = matches_results_df[bookmakers_A].max(axis = 1)

    matches_results_df = matches_results_df.drop(columns = matches_results_df_bookmakers_columns)
    matches_results_df["Date"] = pd.to_datetime(matches_results_df["Date"] , dayfirst = True)
    
    matches_results_df = matches_results_df.replace({
                                           "St. Gilloise" : "Royale Union Saint-Gilloise" ,
                                           "Waregem" : "SV Zulte-Waregem" ,
                                           "Spal" : "SPAL" ,
                                           "St Truiden" : "Sint-Truidense VV" ,
                                           "St Etienne" : "AS Saint-Étienne" ,
                                           "Sp Lisbon" : "Sporting CP" ,
                                           "Dundee" : "Dundee FC" ,
                                           "Barcelona" : "FC Barcelona" ,
                                           "M'gladbach" : "Borussia Mönchengladbach"
                                           })
    
    dictionary = {
    "Div" : "division" ,
    "Date" : "date" ,
    "HomeTeam" : "home_team" ,
    "AwayTeam" : "away_team" ,
    "FTHG" : "full_time_home_goals" ,
    "FTAG" : "full_time_away_goals" ,
    "HS" : "home_shots" ,
    "AS" : "away_shots" ,
    "HST" : "home_shots_on_target" ,
    "AST" : "away_shots_on_target" ,
    "HC" : "home_corners" ,
    "AC" : "away_corners" ,
    "HF" : "home_fouls" ,
    "AF" : "away_fouls" ,
    "HY" : "home_yellows" ,
    "AY" : "away_yellows" ,
    "HR" : "home_reds" ,
    "AR" : "away_reds" ,
    "Max H" : "max_H" ,
    "Max D" : "max_D" ,
    "Max A" : "max_A" ,
    "FTR" : "full_time_result"}
    
    matches_results_df = matches_results_df.rename(columns = dictionary)
    matches_results_df = matches_results_df.reset_index(drop = True)

    return matches_results_df

def get_FIFA_ratings_df(seasons : list , FIFA_files : list):
    input_data_address = "input_data/FIFA_notes/"    
    FIFA_ratings_df = pd.DataFrame()
    selected_columns = ["short_name" , "club_name" , "league_name" , "team_position" , "player_positions" , "age" , "overall" , "potential" , "value_eur" , "pace" , "shooting" , "passing" , "dribbling" , "defending" , "physic"]
    i = 0
    for season , file in zip(seasons , FIFA_files):
        df = pd.read_csv(input_data_address + file , low_memory = False)
        df = df[selected_columns]
        df.insert(0 , "season" , season)
        FIFA_ratings_df = pd.concat([FIFA_ratings_df , df])
        i += 1
    
    FIFA_ratings_df = FIFA_ratings_df.reset_index(drop = True)

    return FIFA_ratings_df

def get_divisions_dictionary(matches_results_df : pd.DataFrame , FIFA_ratings_df : pd.DataFrame):
    print("\nCorrespondance des noms des championnats :\n")
    divisions1 = list(matches_results_df["division"].unique())
    divisions2 = list(FIFA_ratings_df["league_name"].unique())
    divisions2.remove(np.nan)

    divisions_dictionary = {}

    for division1 in divisions1:
        words1 = division1.split(" ")
        max_common_letters = 0
            
        for division2 in divisions2:
            words2 = division2.split(" ")
            common_letters = 0

            for word1 in words1:
                for word2 in words2:
                    for letters in zip(word1 , word2):
                        if (letters[0] == letters[1]):
                            common_letters += 1
                
            if common_letters > max_common_letters:
                max_common_letters = common_letters
                corresponding_division = division2
                
        if corresponding_division in divisions_dictionary.keys():
            for k , v in divisions_dictionary.items():
                if k == corresponding_division:
                    print(k , "/" , v)
                    print(division1 , "/" , corresponding_division)
                raise ValueError("This division has already been assigned !")

        print(corresponding_division , " / " , division1)
        divisions_dictionary[corresponding_division] = division1
    
    return divisions_dictionary

def get_FIFA_ratings_selected_players_df(matches_results_df : pd.DataFrame , FIFA_ratings_df : pd.DataFrame , divisions_dictionary : dict):
    FIFA_ratings_df["league_name"] = FIFA_ratings_df["league_name"].replace(divisions_dictionary)
    FIFA_ratings_selected_players_df = pd.DataFrame()

    for season in matches_results_df["season"].unique():
        for division in matches_results_df[matches_results_df["season"] == season]["division"].unique():  
            for club in FIFA_ratings_df.loc[(FIFA_ratings_df["season"] == season) & (FIFA_ratings_df["league_name"] == division) , "club_name"].unique():
                df = FIFA_ratings_df[(FIFA_ratings_df["club_name"] == club) & (FIFA_ratings_df["season"] == season) & (FIFA_ratings_df["team_position"] != "RES")]
                index = df[(df["team_position"] == "SUB") & (df["player_positions"].str.contains("GK"))].sort_values(by = ["overall" , "potential"] , ascending = False).index[1 :]
                df = df.drop(index = index)
                FIFA_ratings_selected_players_df = pd.concat([FIFA_ratings_selected_players_df , df])
    
    FIFA_ratings_selected_players_df = FIFA_ratings_selected_players_df.drop(FIFA_ratings_selected_players_df[(FIFA_ratings_selected_players_df["season"] == "2019-2020") & (FIFA_ratings_selected_players_df["club_name"] == "Bury")].index)
    FIFA_ratings_selected_players_df = FIFA_ratings_selected_players_df.reset_index(drop = True)
    FIFA_ratings_selected_players_df = FIFA_ratings_selected_players_df.drop(columns = ["short_name" , "team_position" , "player_positions"])
    FIFA_ratings_selected_players_df = FIFA_ratings_selected_players_df.groupby(["season" , "league_name" , "club_name"]).agg(np.nanmean).reset_index()
    FIFA_ratings_selected_players_df.columns = FIFA_ratings_selected_players_df.columns.str.replace("_" , " ")
    
    dictionary = {
    "league name" : "division" ,
    "club name" : "team" ,
    "value eur" : "value"
    }
      
    FIFA_ratings_selected_players_df = FIFA_ratings_selected_players_df.rename(columns = dictionary)
    FIFA_ratings_selected_players_df = FIFA_ratings_selected_players_df.reset_index(drop = True)
    output_data_folder = select_output_data_folder()
    FIFA_ratings_selected_players_df.to_csv(f"{output_data_folder}/FIFA_teams_ratings.csv" , index = False)

    return FIFA_ratings_selected_players_df

def get_clubs_correlation_dictionary(matches_results_df : pd.DataFrame , FIFA_ratings_selected_players_df : pd.DataFrame):
    print("\nNombre d'équipes dans les données de résultats de matchs VS nombre d'équipes dans les données FIFA :\n")
    seasons = matches_results_df["season"].unique()
    seasons_dictionary = {}

    for season in seasons:
        divisions_dictionary = {}
        divisions = matches_results_df[matches_results_df["season"] == season]["division"].unique()
        for division in divisions:
            clubs_temp1 = list(matches_results_df[(matches_results_df["division"] == division) & (matches_results_df["season"] == season)]["home_team"].unique())
            clubs_temp2 = list(matches_results_df[(matches_results_df["division"] == division) & (matches_results_df["season"] == season)]["away_team"].unique())
            clubs1 = set(clubs_temp1 + clubs_temp2)
            clubs2 = FIFA_ratings_selected_players_df[(FIFA_ratings_selected_players_df["division"] == division) & (FIFA_ratings_selected_players_df["season"] == season)]["team"].unique()

            print(season , division , len(clubs1) , len(clubs2))

            if len(clubs1) > len(clubs2):
                raise ValueError("Some teams are not in FIFA !")
            elif len(clubs1) < len(clubs2):
                raise ValueError("Results are missing for some teams !")

            clubs_dictionary = {}

            for club1 in clubs1:
                words1 = club1.split(" ")
                max_common_letters = 0
                min_uncommon_letters = 100

                for club2 in clubs2:
                    words2 = club2.split(" ")
                    common_letters = 0
                    uncommon_letters = 0

                    for word1 in words1:
                        for word2 in words2:
                            for letters in zip(word1 , word2):
                                if (letters[0] == letters[1]):
                                    common_letters += 1
                                else:
                                    uncommon_letters += 1
      
                    if (common_letters > max_common_letters):
                        max_common_letters = common_letters
                        min_uncommon_letters = uncommon_letters
                        corresponding_club = club2
                    elif (common_letters == max_common_letters) & (uncommon_letters < min_uncommon_letters):
                        max_common_letters = common_letters
                        min_uncommon_letters = uncommon_letters
                        corresponding_club = club2
    
                if corresponding_club in clubs_dictionary.values():
                    for k , v in clubs_dictionary.items():
                        if v == corresponding_club:
                            print(k , "/" , v)
                            print(club1 , "/" , corresponding_club)
                            raise ValueError("This team name has already been assigned !")
                else:
                    clubs_dictionary[club1] = corresponding_club
            divisions_dictionary[division] = clubs_dictionary
        seasons_dictionary[season] = divisions_dictionary
    
    return seasons_dictionary

def replace_clubs_names(matches_results_df : pd.DataFrame , clubs_correlation_dictionary : dict):
    for season in matches_results_df["season"].unique():
        for division in matches_results_df[matches_results_df["season"] == season]["division"].unique():
            matches_results_df.loc[(matches_results_df["season"] == season) & (matches_results_df["division"] == division) , "home_team"] = matches_results_df[(matches_results_df["season"] == season) & (matches_results_df["division"] == division)]["home_team"].apply(lambda x: clubs_correlation_dictionary[season][division][x])
            matches_results_df.loc[(matches_results_df["season"] == season) & (matches_results_df["division"] == division) , "away_team"] = matches_results_df[(matches_results_df["season"] == season) & (matches_results_df["division"] == division)]["away_team"].apply(lambda x: clubs_correlation_dictionary[season][division][x])
    output_data_folder = select_output_data_folder()
    matches_results_df.to_csv(f"{output_data_folder}/matches_results.csv" , index = False)
    return matches_results_df

def get_matches_results_corrected_df_and_FIFA_ratings_selected_players_df(seasons : list , FIFA_files : list):
    matches_results_df = get_matches_results_df(seasons = seasons)
    FIFA_ratings_df = get_FIFA_ratings_df(seasons = seasons , FIFA_files = FIFA_files)
    divisions_dictionary = get_divisions_dictionary(matches_results_df = matches_results_df , FIFA_ratings_df = FIFA_ratings_df)
    FIFA_ratings_selected_players_df = get_FIFA_ratings_selected_players_df(matches_results_df = matches_results_df , FIFA_ratings_df = FIFA_ratings_df , divisions_dictionary = divisions_dictionary)
    clubs_correlation_dictionary = get_clubs_correlation_dictionary(matches_results_df = matches_results_df , FIFA_ratings_selected_players_df = FIFA_ratings_selected_players_df)
    matches_results_corrected_df = replace_clubs_names(matches_results_df = matches_results_df , clubs_correlation_dictionary = clubs_correlation_dictionary)

    return matches_results_corrected_df , FIFA_ratings_selected_players_df

def create_tables_matchesresults_and_fifa(session : Session):
    engine = select_engine()
    if (not inspect(engine).has_table("matches_results")) or (not inspect(engine).has_table("FIFA")):
        reset_tables_matchesresults_and_fifa()
        MatchesResultsBase.metadata.create_all(bind = engine)
        FIFABase.metadata.create_all(bind = engine)
        seasons = sorted(listdir("input_data/matches_results"))
        FIFA_files = sorted(listdir("input_data/FIFA_notes"))
        matches_results_corrected_df , FIFA_ratings_selected_players_df = get_matches_results_corrected_df_and_FIFA_ratings_selected_players_df(seasons = seasons , FIFA_files = FIFA_files)
        matches_results_corrected_df.to_sql("matches_results" , if_exists = "append" , con = engine , index = False)
        FIFA_ratings_selected_players_df.to_sql("FIFA" , if_exists = "append" , con = engine , index = False)

def create_table_users(session : Session):
    engine = select_engine()
    if not inspect(engine).has_table("users") :
        UsersBase.metadata.create_all(bind = engine)
        add_to_users_table(Users(username = "Mathieu", password = "Crosnier", is_admin = 1 , registered_date = datetime.now(timezone.utc)) , session = session)

def create_table_predictions(session : Session):
    engine = select_engine()
    if not inspect(engine).has_table("predictions") :
        PredictionsBase.metadata.create_all(bind = engine)

@router.post("/initialize_tables_matchesresults_and_fifa" , name = "Initialize tables MatchesResults and FIFA")
async def initialize_tables_matchesresults_and_fifa(user = Depends(decode_token) , session = Depends(start_session) , engine = Depends(select_engine)):
    if user.get("rights") != "Administrator":
        raise HTTPException(
            status_code = status.HTTP_403_FORBIDDEN ,
            detail = "You must be an administrator to perform this action !" ,
            headers = {"WWW-Authenticate": "Bearer"})
    reset_tables_matchesresults_and_fifa()
    create_tables_matchesresults_and_fifa(session = session)
    
    return "Tables MatchesResults and FIFA have been succesfully initialized !"

@router.post("/initialize_table_users" , name = "Initialize table Users")
async def initialize_table_users(user = Depends(decode_token) , session = Depends(start_session) , engine = Depends(select_engine)):
    if user.get("rights") != "Administrator":
        raise HTTPException(
            status_code = status.HTTP_403_FORBIDDEN ,
            detail = "You must be an administrator to perform this action !" ,
            headers = {"WWW-Authenticate": "Bearer"})
    reset_table_users()
    create_table_users(session = session)

    return "Table Users has been succesfully initialized !"

@router.post("/initialize_table_predictions" , name = "Initialize table Predictions")
async def initialize_table_predictions(user = Depends(decode_token) , session = Depends(start_session) , engine = Depends(select_engine)):
    if user.get("rights") != "Administrator":
        raise HTTPException(
            status_code = status.HTTP_403_FORBIDDEN ,
            detail = "You must be an administrator to perform this action !" ,
            headers = {"WWW-Authenticate": "Bearer"})
    reset_table_predictions()
    create_table_predictions(session = session)

    return "Table Predictions has been succesfully initialized !"