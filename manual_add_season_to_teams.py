import polars as pl

def add_season_to_teams(file_path: str, season: str) -> pl.DataFrame:
    df = pl.read_csv(file_path)
    df = df.with_columns(pl.lit(season).alias("season"))
    return df

if __name__ == "__main__":
    df = add_season_to_teams("src/data/teams_2024_25.csv", "2024-25")
    df.write_csv("src/data/teams_2024_25_with_season.csv")

