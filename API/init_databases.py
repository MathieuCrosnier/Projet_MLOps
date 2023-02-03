import pandas as pd
import numpy as np
from os import listdir
import re
from fastapi import APIRouter , Depends , HTTPException , status
from access import decode_token
from databases import reset_tables , create_tables , start_session , select_engine

router = APIRouter(tags = ["Databases"])

def get_matches_results_df():
    input_data_address = "input_data/matches_results/"
    matches_results_df = pd.DataFrame()
    print("\nLa liste des fichiers traités est la suivante :\n")
    for folder in listdir(input_data_address):

        for file in listdir(input_data_address + "/" + folder):
            print(folder + "/" + file)
            df = pd.read_csv(input_data_address + folder + "/" + file , encoding = 'unicode_escape')
    
            if "Season" in list(df.columns):
                df["Season"] = file.replace(".csv" , "")
            else:
                df.insert(0 , "Season" , file.replace(".csv" , ""))
    
            if "HFKC" in list(df.columns):
                df = df.rename(columns = {"HFKC" : "HF" , "AFKC" : "AF"})
    
            df["Div"] = folder.replace("_" , " ")
            matches_results_df = pd.concat([matches_results_df , df])
    
    matches_results_df = matches_results_df.reset_index(drop = True)    
    matches_results_df = matches_results_df.rename(columns = {"Div" : "Division" , "HomeTeam" : "Home team" , "AwayTeam" : "Away team"})
    matches_statistics = ["Season" , "Division" , "Date" , "Home team" , "Away team" , "FTHG" , "FTAG" , "FTR" , "HS" , "AS" , "HST" , "AST" , "HC" , "AC" , "HF" , "AF" , "HY" , "AY" , "HR" , "AR"]
    bookmakers_list = ["B365H" , "B365D" , "B365A" , "BWH" , "BWD" , "BWA" , "IWH" , "IWD" , "IWA" , "LBH" , "LBD" , "LBA" , "PSH" , "PSD" , "PSA" , "SJH" , "SJD" , "SJA" , "VCH" , "VCD" , "VCA" , "WHH" , "WHD" , "WHA"]
    bookmakers_closing_list = ["B365CH" , "B365CD" , "B365CA" , "BWCH" , "BWCD" , "BWCA" , "IWCH" , "IWCD" , "IWCA" , "PSCH" , "PSCD" , "PSCA" , "VCCH" , "VCCD" , "VCCA" , "WHCH" , "WHCD" , "WHCA"]
    bookmakers_total_list = bookmakers_list + bookmakers_closing_list
    matches_results_df = matches_results_df[matches_statistics + bookmakers_total_list]
    matches_results_df = matches_results_df.drop(matches_results_df[matches_results_df[matches_statistics].isna().any(axis = 1)].index)
    matches_results_df = matches_results_df.drop(matches_results_df[matches_results_df[bookmakers_total_list].isna().all(axis = 1)].index)

    bookmakers_H = []
    bookmakers_D = []
    bookmakers_A = []

    rH = re.compile(r"\w+H$")
    rD = re.compile(r"\w+D$")
    rA = re.compile(r"\w+A$")

    for col in bookmakers_total_list:
    
        if rH.findall(col) != []:
            bookmakers_H.append(rH.findall(col)[0])
            
        elif rD.findall(col) != []:
            bookmakers_D.append(rD.findall(col)[0])
        
        elif rA.findall(col) != []:
            bookmakers_A.append(rA.findall(col)[0])
        
        else:
            raise ValueError

    matches_results_df["Max H"] = matches_results_df[bookmakers_H].max(axis = 1)
    matches_results_df["Max D"] = matches_results_df[bookmakers_D].max(axis = 1)
    matches_results_df["Max A"] = matches_results_df[bookmakers_A].max(axis = 1)

    matches_results_df = matches_results_df.drop(columns = bookmakers_total_list)
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
    "Home team" : "home_team" ,
    "Away team" : "away_team" ,
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

def get_FIFA_ratings_df():
    input_data_address = "input_data/FIFA_notes/"
    seasons = ["2014-2015" , "2015-2016" , "2016-2017" , "2017-2018" , "2018-2019" , "2019-2020" , "2020-2021" , "2021-2022" , "2022-2023"]
    FIFA_ratings_df = pd.DataFrame()
    selected_columns = ["short_name" , "club_name" , "league_name" , "team_position" , "player_positions" , "age" , "overall" , "potential" , "value_eur" , "pace" , "shooting" , "passing" , "dribbling" , "defending" , "physic"]
    i = 0
    for file in sorted(listdir(input_data_address)):
        df = pd.read_csv(input_data_address + file , low_memory = False)
        df = df[selected_columns]
        df.insert(0 , "Season" , seasons[i])
        FIFA_ratings_df = pd.concat([FIFA_ratings_df , df])
        i += 1
    
    FIFA_ratings_df = FIFA_ratings_df.reset_index(drop = True)

    return FIFA_ratings_df

def get_divisions_dictionary(matches_results_df : pd.DataFrame , FIFA_ratings_df : pd.DataFrame):
    divisions1 = list(matches_results_df["Division"].unique())
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
                    print("\nCe nom de division est déjà attribué !")
                raise ValueError

        print(corresponding_division , " / " , division1)
        divisions_dictionary[corresponding_division] = division1
    
    return divisions_dictionary

def get_FIFA_ratings_selected_players_df(matches_results_df : pd.DataFrame , FIFA_ratings_df : pd.DataFrame , divisions_dictionary : dict):
    FIFA_ratings_df["league_name"] = FIFA_ratings_df["league_name"].replace(divisions_dictionary)
    FIFA_ratings_selected_players_df = pd.DataFrame()

    for season in matches_results_df["Season"].unique():
        for division in matches_results_df[matches_results_df["Season"] == season]["Division"].unique():  
            for club in FIFA_ratings_df.loc[(FIFA_ratings_df["Season"] == season) & (FIFA_ratings_df["league_name"] == division) , "club_name"].unique():
                df = FIFA_ratings_df[(FIFA_ratings_df["club_name"] == club) & (FIFA_ratings_df["Season"] == season) & (FIFA_ratings_df["team_position"] != "RES")]
                index = df[(df["team_position"] == "SUB") & (df["player_positions"].str.contains("GK"))].sort_values(by = ["overall" , "potential"] , ascending = False).index[1 :]
                df = df.drop(index = index)
                FIFA_ratings_selected_players_df = pd.concat([FIFA_ratings_selected_players_df , df])
    
    FIFA_ratings_selected_players_df = FIFA_ratings_selected_players_df.drop(FIFA_ratings_selected_players_df[(FIFA_ratings_selected_players_df["Season"] == "2019-2020") & (FIFA_ratings_selected_players_df["club_name"] == "Bury")].index)
    FIFA_ratings_selected_players_df = FIFA_ratings_selected_players_df.reset_index(drop = True)
    FIFA_ratings_selected_players_df = FIFA_ratings_selected_players_df.drop(columns = ["short_name" , "team_position" , "player_positions"])
    FIFA_ratings_selected_players_df = FIFA_ratings_selected_players_df.groupby(["Season" , "league_name" , "club_name"]).agg(np.nanmean).reset_index()
    FIFA_ratings_selected_players_df.columns = FIFA_ratings_selected_players_df.columns.str.replace("_" , " ")
    FIFA_ratings_selected_players_df.columns = FIFA_ratings_selected_players_df.columns.str.capitalize()
    
    dictionary = {
    "League name" : "division" ,
    "Club name" : "team" ,
    "Value eur" : "value"
    }
      
    FIFA_ratings_selected_players_df = FIFA_ratings_selected_players_df.rename(columns = dictionary)
    FIFA_ratings_selected_players_df = FIFA_ratings_selected_players_df.reset_index(drop = True)
    FIFA_ratings_selected_players_df.to_csv("output_data/FIFA_teams_ratings.csv" , index = False)

    return FIFA_ratings_selected_players_df

def get_clubs_correlation_dictionary(matches_results_df : pd.DataFrame , FIFA_ratings_selected_players_df : pd.DataFrame):
    seasons = matches_results_df["Season"].unique()
    seasons_dictionary = {}

    for season in seasons:
        divisions_dictionary = {}
        divisions = matches_results_df[matches_results_df["Season"] == season]["Division"].unique()
        for division in divisions:
            clubs_temp1 = list(matches_results_df[(matches_results_df["Division"] == division) & (matches_results_df["Season"] == season)]["home_team"].unique())
            clubs_temp2 = list(matches_results_df[(matches_results_df["Division"] == division) & (matches_results_df["Season"] == season)]["away_team"].unique())
            clubs1 = set(clubs_temp1 + clubs_temp2)
            clubs2 = FIFA_ratings_selected_players_df[(FIFA_ratings_selected_players_df["division"] == division) & (FIFA_ratings_selected_players_df["Season"] == season)]["team"].unique()
    
            print(season , division , len(clubs1) , len(clubs2))

            if len(clubs1) > len(clubs2):
                raise ValueError("Certains clubs ne sont pas présents dans le jeu FIFA !")
            elif len(clubs1) < len(clubs2):
                raise ValueError("Attention, il manque les résultats de certaines équipes !")

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
                            raise ValueError("Ce nom de club est déjà attribué !")
                else:
                    clubs_dictionary[club1] = corresponding_club
            divisions_dictionary[division] = clubs_dictionary
        seasons_dictionary[season] = divisions_dictionary
    
    return seasons_dictionary

def replace_clubs_names(matches_results_df : pd.DataFrame , clubs_correlation_dictionary : dict):
    for season in matches_results_df["Season"].unique():
        for division in matches_results_df[matches_results_df["Season"] == season]["Division"].unique():
            matches_results_df.loc[(matches_results_df["Season"] == season) & (matches_results_df["Division"] == division) , "home_team"] = matches_results_df[(matches_results_df["Season"] == season) & (matches_results_df["Division"] == division)]["home_team"].apply(lambda x: clubs_correlation_dictionary[season][division][x])
            matches_results_df.loc[(matches_results_df["Season"] == season) & (matches_results_df["Division"] == division) , "away_team"] = matches_results_df[(matches_results_df["Season"] == season) & (matches_results_df["Division"] == division)]["away_team"].apply(lambda x: clubs_correlation_dictionary[season][division][x])
    matches_results_df.to_csv("output_data/matches_results.csv" , index = False)
    return matches_results_df

@router.post("/initialize_databases" , name = "Initialize databases")
async def initialize_tables(user = Depends(decode_token) , session = Depends(start_session) , engine = Depends(select_engine)):
    if user.get("rights") != "Administrator":
        raise HTTPException(
            status_code = status.HTTP_403_FORBIDDEN ,
            detail = "You must be an administrator to perform this action." ,
            headers = {"WWW-Authenticate": "Bearer"})
    reset_tables()
    create_tables(session = session)
    matches_results_df = get_matches_results_df()
    FIFA_ratings_df = get_FIFA_ratings_df()
    divisions_dictionary = get_divisions_dictionary(matches_results_df = matches_results_df , FIFA_ratings_df = FIFA_ratings_df)
    FIFA_ratings_selected_players_df = get_FIFA_ratings_selected_players_df(matches_results_df = matches_results_df , FIFA_ratings_df = FIFA_ratings_df , divisions_dictionary = divisions_dictionary)
    clubs_correlation_dictionary = get_clubs_correlation_dictionary(matches_results_df , FIFA_ratings_selected_players_df)
    matches_results_corrected_df = replace_clubs_names(matches_results_df , clubs_correlation_dictionary)

    matches_results_corrected_df.to_sql("matches_results" , con = engine , if_exists = "append" , index = False)
    FIFA_ratings_selected_players_df.to_sql("FIFA" , con = engine , if_exists = "append" , index = False)

    return "Les bases de données ont été réinitialisées avec succès !"