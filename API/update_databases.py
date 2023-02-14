from databases import select_engine
from init_databases import get_matches_results_corrected_df_and_FIFA_ratings_selected_players_df
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

def update_matches_results_table(seasons : list , FIFA_files : list):
    matches_results_corrected_df , _ = get_matches_results_corrected_df_and_FIFA_ratings_selected_players_df(seasons = seasons , FIFA_files = FIFA_files)

    engine = select_engine()

    datas = matches_results_corrected_df.to_dict(orient='records')
    conn = engine.connect()
    for data in datas:
        try:
            query = text("INSERT INTO matches_results (season , division , date , home_team , away_team , full_time_home_goals , full_time_away_goals , home_shots , away_shots , home_shots_on_target , away_shots_on_target , home_corners , away_corners , home_fouls , away_fouls , home_yellows , away_yellows , home_reds , away_reds , max_H , max_D , max_A , full_time_result) VALUES (:Season , :Division , :Date , :home_team , :away_team , :full_time_home_goals , :full_time_away_goals , :home_shots , :away_shots , :home_shots_on_target , :away_shots_on_target , :home_corners , :away_corners , :home_fouls , :away_fouls , :home_yellows , :away_yellows , :home_reds , :away_reds , :max_H , :max_D , :max_A , :full_time_result)")
            conn.execute(query , data)
        except IntegrityError:
            pass
    conn.commit()
    conn.close()