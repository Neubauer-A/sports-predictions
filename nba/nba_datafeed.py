import os

from nba_api.stats.endpoints import boxscoretraditionalv2
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.endpoints import playergamelog
from nba_api.stats.static import teams
import pandas as pd


class NBADataFeed:
    """
    Object for ingesting data from the NBA's API.
    Args:
        player_games_dir(str):       directory used for player data
        team_games_dir(str):         directory used for team data
        preexisting_games_path(str): path for data used to check which games
                                     need to be updated for player datasets
    """
    def __init__(self, player_games_dir, team_games_dir, preexisting_games_path):
        self.player_games_dir = player_games_dir
        self.team_games_dir = team_games_dir
        self.preexisting_games_path = preexisting_games_path

    def get_team_games(self, team_id):
        """
        Make an API call to the NBA API to get game-level stats by team.
        Args:
            team_id(str): the ID of the team whose data to return
        Returns:
            df(DataFrame): dataframe of game-level stats
        """
        # API call
        game_finder = leaguegamefinder.LeagueGameFinder(
            team_id_nullable=team_id
        )
        # Get team game stats, drop nulls, sort by game date, drop unneeded data
        df = game_finder.get_data_frames()[0] \
                        .dropna() \
                        .sort_values("GAME_DATE") \
                        .drop(columns=[
                            'FG3_PCT', 'FG_PCT', 'FT_PCT', 'MIN', 'REB'
                        ])
        return df

    def get_all_team_games(self):
        """Create/update game-level datasets for all current NBA teams."""
        # Get a dataset of all teams
        nba_teams = pd.DataFrame(teams.get_teams())
        nba_teams.columns = [f"team_{col}" for col in nba_teams.columns]

        # For each team, build a game-level dataset
        for i in range(len(nba_teams)):
            row = nba_teams.iloc[i]
            team_id = row["team_id"]
            df = self.get_team_games(team_id)
            path = f"{self.player_games_dir}/{df.iloc[0]['TEAM_ID']}.csv"
            # Check if we have existing data that we can add to and deduplicate
            if os.path.exists(path):
                old_df = pd.read_csv(path)
                df = pd.concat([old_df, df]).drop_duplicates()
            # Save the data
            df.to_csv(path, index=False)

    def get_all_games(self):
        """
        Get a dataset of all game IDs.
        Returns:
            df(DataFrame): dataframe of game IDs
        """
        # Iterate through current team datasets to get all game IDs
        dfs = []
        for i in os.listdir(self.team_games_dir):
            # keep only the game IDs
            dfs.append(
                pd.read_csv(
                    self.team_games_dir+i,
                    dtype={'GAME_ID': str}
                )["GAME_ID"])
        df = pd.concat(dfs).drop_duplicates()
        return df

    def player_games_to_update(self):
        """
        Get a dataset of game IDs to be used for player dataset updates.
        Returns:
            df(DataFrame): dataframe of game IDs for player data updates
        """
        # get all games, keep only those that weren't in the preexisting data
        all_games = self.get_all_games()
        preexisting_games = pd.read_csv(
            self.preexisting_games_path,
            dtype={'GAME_ID': str}
        )
        df = pd.DataFrame(
            all_games[~all_games.isin(preexisting_games["GAME_ID"])]
        )
        return df

    def update_preexisting_games(self, update_df):
        """
        Update existing preexisting games file.
        Args:
            update_df(DataFrame): dataset to use for update
        """
        preexisting_games = pd.read_csv(
            self.preexisting_games_path,
            dtype={'GAME_ID': str}
        )
        df = pd.concat([preexisting_games, update_df]).drop_duplicates()
        df.to_csv(self.preexisting_games_path, index=False)

    def get_player_ids_by_game(self, game_id):
        """
        Get a list of player IDs whose game-level data needs to be updated.
        Args:
            game_id(str): game whose players to get a list of
        Returns:
            player_ids(list): list of IDs for players in the game
        """
        # Fetch the boxscore data for the specified game
        boxscore = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
        player_ids = boxscore.get_data_frames()[0]["PLAYER_ID"].values.tolist()
        return player_ids

    def get_player_games(self, player_id):
        """
        Make an API call to the NBA API to get game-level stats by player.
        Args:
            player_id(str): the ID of the player whose data to return
        Returns:
            df(DataFrame): dataframe of game-level stats
        """
        # Fetch player game log data
        game_log = playergamelog.PlayerGameLog(
            player_id=player_id, season="ALL"
        )
        # Get player game stats, drop nulls, sort by game date,
        # drop unneeded data, and rename columns
        df = game_log.get_data_frames()[0] \
                     .dropna() \
                     .sort_values("GAME_DATE") \
                     .drop(columns=[
                         'FG3_PCT', 'FG_PCT', 'FT_PCT',
                         'MIN', 'REB', 'VIDEO_AVAILABLE'
                     ]) \
                     .rename(columns={
                         "Player_ID":"PLAYER_ID",
                         "Game_ID":"GAME_ID"
                     })
        # ensure player ID is present
        if "PLAYER_ID" not in df.columns:
            df["PLAYER_ID"] = str(player_id)
            df = df[[
                'SEASON_ID', 'PLAYER_ID', 'GAME_ID', 'GAME_DATE', 'MATCHUP',
                'WL', 'FGM', 'FGA', 'FG3M', 'FG3A', 'FTM', 'FTA', 'OREB',
                'DREB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', 'PLUS_MINUS'
            ]]
        return df

    def get_all_player_games(self, player_ids):
        """
        Create/update game-level datasets for NBA players.
        Args:
            players_ids(set): IDs of players whose data needs updating
        """
        # For each player, build a game-level dataset
        for player_id in player_ids:
            player_id = str(player_id)
            df = self.get_player_games(player_id)
            if len(df) == 0:
                continue
            path = f"{self.player_games_dir}/{player_id}.csv"
            # Check if we have existing data that we can add to and deduplicate
            if os.path.exists(path):
                old_df = pd.read_csv(path)
                df = pd.concat([old_df, df]).drop_duplicates()
            df.to_csv(path, index=False)

    def update_player_games(self):
        """Update all player game-level datasets."""
        # get game IDs to use for updates
        games_to_update = self.player_games_to_update()
        # get unique player IDs from all games
        player_ids = set()
        for game_id in games_to_update["GAME_ID"].values:
            player_ids.update(self.get_player_ids_by_game(game_id))
        # update all player datasets
        self.get_all_player_games(player_ids)
        # overwrite preexisting games file
        self.update_preexisting_games(games_to_update)

    def run(self):
        """Run ingest to create/update all team and player data."""
        self.get_all_team_games()
        self.update_player_games()
