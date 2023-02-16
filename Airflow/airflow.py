from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.github.operators.github import GithubOperator
import urllib.request
from datetime import timedelta
from os import listdir , mkdir
import pandas as pd
import re
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import KNeighborsClassifier
from joblib import dump 

dag = DAG(
    dag_id = "matches_results_download" ,
    tags = ["SportsBetPy"] ,
    schedule_interval = timedelta(days = 1),
    catchup = False,
    default_args = {
        "owner" : "crocro57" ,
        "start_date" : days_ago(0)
})

def download_matches_results_files():
    base_url = "https://www.football-data.co.uk/mmz4281/2223/"
    base_folder = "/mnt/c/Users/matcr/Documents/GitHub/Projet_MLOps/API/input_data/matches_results/2022-2023/"
    divisions = {
        "B1" : "Belgian_Jupiler_Pro_League.csv" ,
        "E0" : "English_Premier_League.csv" ,
        "E1" : "English_League_Championship.csv" ,
        "E2" : "English_League_One.csv" ,
        "E3" : "English_League_Two.csv" ,
        "F1" : "French_Ligue_1.csv" ,
        "F2" : "French_Ligue_2.csv" ,
        "D1" : "German_1._Bundesliga.csv" ,
        "D2" : "German_2._Bundesliga.csv" ,
        "N1" : "Holland_Eredivisie.csv" ,
        "I1" : "Italian_Serie_A.csv" ,
        "I2" : "Italian_Serie_B.csv" ,
        "P1" : "Portuguese_Liga_ZON_SAGRES.csv" ,
        "SC0" : "Scottish_Premiership.csv" ,
        "SP1" : "Spanish_Primera_Division.csv" ,
        "SP2" : "Spanish_Segunda_Division.csv" ,
        "T1" : "Turkish_Super_Lig.csv"}

    for url , fichier in divisions.items():
        urllib.request.urlretrieve(url = base_url + url, filename = base_folder + fichier)

def select_output_data_folder(folder : str = "output_data" , subfolder : str = "production" , API_path : str = "/mnt/c/Users/matcr/Documents/GitHub/Projet_MLOps/API/"):
    if folder not in listdir(API_path):
        mkdir(API_path + folder)
    if subfolder not in listdir(API_path + folder):
        mkdir(API_path + folder + "/" + subfolder)
        
    return API_path + folder + "/" + subfolder

def get_matches_results_df(seasons : list , input_data_address : str = "/mnt/c/Users/matcr/Documents/GitHub/Projet_MLOps/API/input_data/matches_results/"):
    matches_results_df = pd.DataFrame()
    print("\nLa liste des fichiers traités est la suivante :\n")
    for folder in seasons:

        for file in listdir(input_data_address + folder):
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

def get_FIFA_ratings_df(seasons : list , FIFA_files : list , input_data_address : str = "/mnt/c/Users/matcr/Documents/GitHub/Projet_MLOps/API/input_data/FIFA_notes/"):    
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
    print(matches_results_df.head())
    print(FIFA_ratings_selected_players_df.head())
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

def get_ml_df(seasons : list , FIFA_files : list):
    matches_results_corrected_df , FIFA_ratings_selected_players_df = get_matches_results_corrected_df_and_FIFA_ratings_selected_players_df(seasons = seasons , FIFA_files = FIFA_files)
    
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
    output_data_folder = select_output_data_folder()
    temp_df.to_csv(f"{output_data_folder}/temp_df.csv" , index = False)
    df_matches_results_columns = ["season" , "division" , "date" , "home_team" , "away_team" , "full_time_home_goals" , "full_time_away_goals" , "home_shots" , "away_shots" , "home_shots_on_target" , "away_shots_on_target" , "home_fouls" , "away_fouls" , "home_corners" , "away_corners" , "home_yellows" , "away_yellows" , "home_reds" , "away_reds" , "max_H" , "max_D" , "max_A" , "full_time_result"]
    df_matches_results = temp_df[df_matches_results_columns]
    temp_df = temp_df.drop(columns = df_matches_results.columns)
    df_home = temp_df.loc[: , temp_df.columns[temp_df.columns.str.contains("home_")]]
    df_away = temp_df.loc[: , temp_df.columns[temp_df.columns.str.contains("away_")]]
    df_home.columns = [x.replace("home_" , "") for x in df_home.columns]
    df_away.columns = [x.replace("away_" , "") for x in df_away.columns]
    df = df_matches_results.join(df_home - df_away)
    df = df.reset_index(drop = True)

    df.to_csv(f"{output_data_folder}/complete_dataset.csv" , index = False)

    df = df.drop(columns = df_matches_results.columns.drop("full_time_result"))
    df = df.reset_index(drop = True)
    print(f"\nLe dataframe comporte {df.shape[0]} matches.")
    print(f"\nLe dataframe comporte {df.isna().sum().sum()} valeurs manquantes.")
    df.to_csv(f"{output_data_folder}/final_dataset.csv" , index = False)

    return df

def train_model(seasons : list , FIFA_files : list):
    ml_df = get_ml_df(seasons = seasons , FIFA_files = FIFA_files)
    X_train = ml_df.drop(columns = "full_time_result")
    y_train = ml_df["full_time_result"]
    scaler = StandardScaler().fit(X_train)
    X_train_scaled = pd.DataFrame(scaler.transform(X_train) , index = X_train.index , columns = X_train.columns)
    output_data_folder = select_output_data_folder()
    X_train.to_csv(f"{output_data_folder}/X_train.csv")
    X_train_scaled.to_csv(f"{output_data_folder}/X_train_scaled.csv")
    model = KNeighborsClassifier(n_neighbors = 134)
    model.fit(X_train_scaled , y_train)
    dump(model , f"{output_data_folder}/model.pkl")
    dump(scaler , f"{output_data_folder}/scaler.pkl")

task1 = PythonOperator(
    task_id = "download_matches_results_files" ,
    python_callable = download_matches_results_files ,
    dag = dag)

task2 = PythonOperator(
    task_id = "train_model" ,
    python_callable = train_model ,
    op_kwargs= {
        "seasons" : sorted(listdir("/mnt/c/Users/matcr/Documents/GitHub/Projet_MLOps/API/input_data/matches_results/")) ,
        "FIFA_files" : sorted(listdir("/mnt/c/Users/matcr/Documents/GitHub/Projet_MLOps/API/input_data/FIFA_notes/"))
    } ,
    dag = dag)

git_add_commit_push_commands = """
cd /mnt/c/Users/matcr/Documents/GitHub/Projet_MLOps/API ;
git add input_data ;
git add output_data ;
git commit -m "Update of matches results and training of the model by Airflow" ;
git push ;
"""

task3 = BashOperator(
    task_id = "git_add_commit_push" ,
    bash_command = git_add_commit_push_commands ,
    dag = dag)

task4 = GithubOperator(
    task_id = "github_pull_request" ,
    github_conn_id = "GitHub" ,
    github_method = "get_repo" ,
    github_method_args = {"full_name_or_id" : "MathieuCrosnier/Projet_MLOps"} ,
    result_processor = lambda repo : repo.create_pull(title = "Pull request from Airflow" , body = "Update of matches results and training of the model" , head = "Development" , base = "main") ,
    dag = dag)

task1 >> task2 >> task3 >> task4