# SportsBetPy
- [Presentation](#presentation)
- [Main features](#main-features)
  - [REST API](#rest-api)
  - [Databases](#databases)
    - [Matches_results](#matches_results)
    - [FIFA](#fifa)
    - [Users](#users)
    - [Predictions](#predictions)
  - [Airflow](#airflow)
  - [Tests](#tests)
  - [Docker](#docker)
- [Getting started](#getting-started)

## Presentation
This GitHub repository contains all the elements to deploy the [SportsBetPy](https://github.com/MathieuCrosnier/SportsBetPy) project developed in the framework of the DataScientest training.
SportsBetPy is an application that allows a bettor to compute the odds of a football game, with an objective to help him identify the profitable bets by comparing the computed odds with the bookmakers odds. For more information about the aim of the project, please refer to the following [report](https://github.com/MathieuCrosnier/SportsBetPy/blob/main/SportsBetPy.pdf).

## Main features
### REST API
The main feature is an API developed with FastAPI.
The API allows to perform 7 different requests :

- /signup

This endpoint allows to create an account by typing a username and a password. The new user is created with standard rights. Users are stored in a SQL table called [users](#users).

Nota: A user with username "Mathieu" and password "Crosnier" and with administrator rights exists by default.

- /token

When you log in to the API, this endpoint generates a JWT token. It allows to safely identify who got connected and to keep this information until the token expires.

- /user

This endpoint displays, for the logged in user, it's username and rights.

- /prediction

This endpoint allows to make a prediction using the Machine Learning model. The user must type in the 2 teams playing the game and the game date (with yyyy-mm-dd format). The result is the computed odds. It allows to compare to the bookmakers odds and decide whether or not a bet is profitable. For more information on the theory behind odds, please refer to the following [report](https://github.com/MathieuCrosnier/SportsBetPy/blob/main/SportsBetPy.pdf).
Predictions are stored in a SQL table called [predictions](#predictions).
The list of teams with the correct syntax from current season for the different divisions can be found in the following [file](https://github.com/MathieuCrosnier/Projet_MLOps/tree/main/Current_season_teams.xlsx).

- /initialize_tables_matchesresults_and_fifa

This endpoint allows to reset both tables [matches_results](#matches_results) and [FIFA](#fifa).
It requires administrator rights.

- /initialize_table_users

This endpoint allows to reset the table [users](#users).
It requires administrator rights.

- /initialize_table_predictions

This endpoint allows to reset the table [predictions](#predictions).
It requires administrator rights.

All the files related to the API are stored in the folder [API](https://github.com/MathieuCrosnier/Projet_MLOps/tree/main/API).

### Databases
Some important data needed to use the API is stored in SQL tables via MySQL.

4 tables are used:
#### Matches_results
This table contains the matches results from season 2014-2015 until now, for the 17 following division:
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

Data is taken from https://www.football-data.co.uk/data.php website and stored through csv files in the folder [API/input_data/matches_results](https://github.com/MathieuCrosnier/Projet_MLOps/tree/main/API/input_data/matches_results). 

When starting up the API, this table is updated with the potential new matches results.
#### FIFA

This table contains the information about the teams for the seasons and divisions available:
- season: season when the team played,
- division: competition in which the team played,
- team: name of the team,
- age: average age of the team,
- overall: average global note of the team (out of 100),
- potential: average global note of the team considering it's room for improvement (out of 100), 
- value: average price of the team on the transfer market (in €),
- pace: average pace note of the team (out of 100),
- shooting: average shooting note of the team (out of 100),
- passing: average passing note of the team (out of 100),
- dribbling: average dribbling note of the team (out of 100),
- defending: average defending note of the team (out of 100),
- physic: average physical note of the team (out of 100).

Data is taken from the FIFA video games and stored through csv files in the folder [API/input_data/FIFA_notes](https://github.com/MathieuCrosnier/Projet_MLOps/tree/main/API/input_data/FIFA_notes).

#### Users

This table contains the information about the registered users. All the users created with the /signup endpoint are stored in this table. A user with username "Mathieu" and password "Crosnier" and with administrator rights exists by default. The information stored in this table is the following:
- username: the user's username,
- password: the user's password,
- is_admin: the user's rights (standard or administrator),
- registered_date: the user's date of registration.

#### Predictions

This table contains the predictions made with the /prediction endpoint. The information stored in this table is the following:
- username: the username of the user who made the prediction,
- home_team: the home team name for the predicted game, 
- away_team: the away team name for the predicted game,
- game_date: the date of the predicted game,
- home_odd_predicted: the odd for the home team win returned by the model,
- draw_odd_predicted: the the odd for the draw returned by the model,
- away_odd_predicted: the odd for the away team win returned by the model,
- prediction_date: the date of the prediction.

### Airflow

Airflow is used to automatize some recurrent tasks needed to keep the project updated.
The tasks Airflow performs are the following:
- Downloading the latest matches results for all the divisions,
- Treat the data to obtain the Machine Learning dataset,
- Compute the metric. The metric considered is the ROI (Return Over Investment) as if we were betting money following the Machine Learning model. For more information about this metric, please refer to the following [report](https://github.com/MathieuCrosnier/SportsBetPy/blob/main/SportsBetPy.pdf). The metric is stored as an XCom in Airflow.
- Train the Machine Learning model. The model is then stored in the folder [API/output_data/production](https://github.com/MathieuCrosnier/Projet_MLOps/tree/main/API/output_data/production),
- Add, commit and push to GitHub,
- Perform a Pull Request on the main branch.

The airflow file can be found in the folder [Airflow](https://github.com/MathieuCrosnier/Projet_MLOps/tree/main/Airflow).

### Tests

On Pull Requests, a [GitHub workflow](https://github.com/MathieuCrosnier/Projet_MLOps/tree/main/.github/workflows/tests_workflow.yml) starts and executes a batch of tests on the API using pytest.
If the tests pass, the Pull Request can be validated and the main branch is updated.

### Docker

When a push is made on the main branch, a [GitHub workflow](https://github.com/MathieuCrosnier/Projet_MLOps/tree/main/.github/workflows/docker_workflow.yml) starts and executes some commands to build the API image.
The [Dockerfile](https://github.com/MathieuCrosnier/Projet_MLOps/tree/main/Dockerfile) used to build the image is stored in the root folder.
The image is then pushed to [Docker Hub](https://hub.docker.com/r/crocro57/image_api).

## Getting started
