# SportsBetPy
- [Presentation](#presentation)
- [Main features](#main-features)
  - [REST API](#rest-api)

## Presentation
This GitHub repository contains all the elements to deploy the [SportsBetPy](https://github.com/MathieuCrosnier/SportsBetPy) project developed in the framework of the DataScientest training.
SportsBetPy is an application that allows a bettor to compute the odds of a football game, with an objective to help him identify the profitable bets by comparing the computed odds with the bookmakers odds.

## Main features
### REST API
The main feature is an API developed with FastAPI.
The API allows to perform 8 different requests :
#### Access
- /signup
This endpoint allows to create an account by typing a username and a password. The new user is created with standard rights. Users are stored in a SQL table called [users](#users).
Nota: A user with username "Mathieu" and password "Crosnier" and with administrator rights exists by default.

- /token
When you log in to the API, this endpoint generates a JWT token. It allows to safely  identify who got connected and to keep this information until the token expires.

- /user
This endpoint displays, for the logged in user, it's username and rights.

#### Prediction
- /prediction
This endpoint allows to make a prediction using the Machine Learning model. The user must type in the 2 teams playing the game and the game date (with yyyy-mm-dd format). The result is the computed odds. It allows to compare to the bookmakers odds and decide whether or not a bet is profitable. For more information on the theory behind odds, please refer to the following [report](https://github.com/MathieuCrosnier/SportsBetPy/blob/main/SportsBetPy.pdf).
Predictions are stored in a SQL table called [predictions](#predictions).

#### Databases
- /initialize_tables_matchesresults_and_fifa
This endpoint allows to reset both tables [matches_results](#matchesresults) and [FIFA]#(fifa).

### Databases
Some important data, needed to use the API, is stored in SQL tables via MySQL.
4 tables are used:
-	Matches_results: this table contains the matches results from season 2014-2015 until now, for the 17 following division:
  -	Belgian Jupiler Pro League,
  -	English Premier League
  -	English League Championship,
  -	English League One,
  -	English League Two,
  -	French Ligue 1,
  -	French Ligue 2,
  -	German 1. Bundesliga,
  -	German 2. Bundesliga,
  -	Holland Eredivisie,
  -	Italian Serie A,
  -	Italian Serie B,
  -	Portuguese Liga ZON SAGRES,
  -	Scottish Premiership,
  -	Spanish Primera Division,
  -	Spanish Segunda Division,
  -	Turkish Super Lig.

  For every game, the following information is stored:
  -	season: season when the match was played,
  -	division: competition in which the match was played,
  -	date: date of the match,
  -	home_team: name of the home team,
  -	away_team: name of the away team,
  -	full_time_home_goals: number of goals scored by the home team,
  -	full_time_away_goals: number of goals scored by the away team,
  -	home_shots: number of shots made by the home team, 
  -	away_shots: number of shots made by the away team,
  -	home_shots_on_target: number of shots on target made by the home team,
  -	away_shots_on_target: number of shots on target made by the away team,
  -	home_corners: number of corners obtained by the home team,
  -	away_corners: number of corners obtained by the away team,
  -	home_fouls: number of fouls committed by the home team,
  -	away_fouls: number of fouls committed by the away team,
  -	home_yellows: number of yellow cards given to home team,
  -	away_yellows: number of yellow cards given to away team,
  -	home_reds: number of red cards given to home team,
  -	away_reds: number of red cards given to away team,
  -	max_H: maximum bookmaker odd for home team win,
  -	max_D: maximum bookmaker odd for draw,
  -	max_A: maximum bookmaker odd for away team win,
  -	full_time_result: the final result of the match.
