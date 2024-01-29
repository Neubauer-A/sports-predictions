import os
import warnings

import pandas as pd
from pandas.errors import SettingWithCopyWarning
from pandas.errors import PerformanceWarning

warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)
warnings.simplefilter(action="ignore", category=PerformanceWarning)


class NBAAnalyzer:
    def __init__(self, player_games_dir, team_games_dir, analyzed_games_path):
        self.player_games_dir = player_games_dir
        self.team_games_dir = team_games_dir
        self.analyzed_games_path = analyzed_games_path
        self.player_df = None
        self.team_df = None
        self.merged_data = None

    def get_game_averages_df(self, df, team=False, player=False):
        # indicate inclusive season, win/loss column to int, home game
        df["SEASON_YEAR"] = df["SEASON_ID"].str[-4:]
        df["WL"] = df["WL"].apply(lambda x: 1 if x=="W" else 0)
        df["HOME_GAME"] = df["MATCHUP"].apply(lambda x: 0 if '@' in x else 1)
        df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"])
        df = df.sort_values("GAME_DATE")
        df["REST_DAYS"] = df["GAME_DATE"].diff().dt.days.fillna(0)

        # columns we want to calculate averages for
        stats_cols = [col for col in df.columns if col not in [
            "SEASON_ID", "TEAM_ID", "PLAYER_ID", "TEAM_ABBREVIATION",
            "TEAM_NAME", "GAME_ID", "SEASON_YEAR", "HOME_GAME", "GAME_DATE",
            "MATCHUP"
        ]]

        rows = []
        for season in df["SEASON_YEAR"].unique():
          # Get stats columns for games in the specific season
          season_games = df[df["SEASON_YEAR"]==season][stats_cols]

          # Get averages over the season
          season_rows = [season_games.iloc[0]]
          cumulative_row = season_games.iloc[0]
          games_played = 1
          for i in range(1, len(season_games)):
              games_played += 1
              row = season_games.iloc[i]
              cumulative_row = cumulative_row + row
              season_rows.append(cumulative_row / games_played)

          rows += season_rows

        stats_df = pd.DataFrame(rows)
        stats_df.index = df.index
        avg_columns = [f"SEASON_AVG_{col}" for col in stats_df.columns]
        stats_df.columns = avg_columns

        # Merge basic data from API with averages columns
        df = df.reset_index()
        stats_df = stats_df.reset_index()
        stats_df = pd.merge(df, stats_df, on="index").drop(columns="index")
        keep_cols = []
        if team:
            keep_cols += ["TEAM_ABBREVIATION", "TEAM_ID"]
        if player:
            keep_cols += ["PLAYER_ID"]
        keep_cols += ["GAME_ID", "HOME_GAME"] + stats_cols + avg_columns
        stats_df = stats_df[keep_cols]

        return stats_df

    def load_and_get_averages(self, team=False, player=False):
        # set formatting variables
        if team:
            dir = self.team_games_dir
            id_prefix = "TEAM"
        elif player:
            dir = self.player_games_dir
            id_prefix = "PLAYER"

        dfs = []
        # read all dfs
        for i in os.listdir(dir):
            if not i.endswith(".csv"):
                continue
            df = pd.read_csv(
                f"{dir}/{i}",
                dtype={
                    "GAME_ID": str,
                    "SEASON_ID": str,
                    f"{id_prefix}_ID": str
                }
            )
            # get averages columns
            df = self.get_game_averages_df(df, team=team, player=player)
            dfs.append(df)
        # concat all dfs
        df = pd.concat(dfs)

        if team:
            self.team_df = df
        elif player:
            self.player_df = df

    def update_player_df(self):
        # columns we don't want stats for
        exclude_cols = ['PLAYER_ID', 'GAME_ID', 'HOME_GAME', 'WL']
        stats_cols = [col for col in self.player_df.columns if col not in exclude_cols]
        # columns we want in the final distribution dataframe
        dist_cols = ["GAME_ID", "HOME_GAME", "WL"]
        for col in stats_cols:
            for suffix in ["_mean", "_min", "_q25", "_q50", "_q75", "_max"]:
                dist_cols.append(col+suffix)

        # load previously analyzed games
        analyzed_games_df = pd.read_csv(
            self.analyzed_games_path,
            dtype={"GAME_ID": str}
        )
        # check which games need to be analyzed
        game_ids = set(self.player_df["GAME_ID"].unique()) - \
                   set(analyzed_games_df["GAME_ID"].unique())

        # add newly analyzed games to the list
        dfs = [analyzed_games_df]
        for game_id in game_ids:
            # get only the data for this particular game
            game_df = self.player_df[self.player_df["GAME_ID"] == game_id]
            # separate the teams
            df_home = game_df[game_df["HOME_GAME"] == 1]
            df_away = game_df[game_df["HOME_GAME"] == 0]
            # for each team, get distribution stats and append row to dfs
            for team_df in [df_home, df_away]:
                for col in stats_cols:
                    team_df[col+"_mean"] = team_df[col].mean()
                    team_df[col+"_min"] = team_df[col].min()
                    team_df[col+"_q25"] = team_df[col].quantile(0.25)
                    team_df[col+"_q50"] = team_df[col].quantile(0.5)
                    team_df[col+"_q75"] = team_df[col].quantile(0.75)
                    team_df[col+"_max"] = team_df[col].max()
                team_df = team_df[dist_cols].drop_duplicates()
                dfs.append(team_df)
        # concat into updated analyzed games dataframe and save
        df = pd.concat(dfs)
        df.to_csv(self.analyzed_games_path, index=False)
        self.player_df = df

    def clean_and_merge_dfs(self):
        # ensure we only get games with data for the home and away teams
        game_id_counts = self.team_df.groupby('GAME_ID')['GAME_ID'].transform('count')
        df = self.team_df[game_id_counts == 2]
        # merge teams with players
        df = pd.merge(df, self.player_df, on=["GAME_ID", "HOME_GAME", "WL"])
        # separate prev and curr home game features and create target column
        df = df.sort_values(by=["TEAM_ID","GAME_ID"])
        df[["GAME_ID_JOIN", "HOME_GAME_CURR", "WL_PRED"]] = \
            df.groupby("TEAM_ID")[["GAME_ID", "HOME_GAME", "WL"]].shift(-1)
        # split into home and away dfs
        home_df = df[df["HOME_GAME"]==1]
        away_df = df[df["HOME_GAME"]==0]
        # merge into game rows
        df = pd.merge(
            home_df, away_df, on="GAME_ID_JOIN", suffixes=("_HOME", "_AWAY")
        )
        # drop unecessary columns
        drop = [col for col in df.columns if \
                col.startswith("TEAM_ID") or \
                col.startswith("GAME_ID") or \
                col == "WL_PRED_AWAY"]
        df = df.drop(columns=drop)
        self.merged_data = df

    def run(self):
        self.load_and_get_averages(team=True)
        self.load_and_get_averages(player=True)
        self.update_player_df()
        self.clean_and_merge_dfs()
