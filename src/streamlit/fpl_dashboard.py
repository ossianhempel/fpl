import streamlit as st
import polars as pl
import pandas as pd
import os
import sys
import plotly.express as px
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from dotenv import load_dotenv
import plotly.graph_objects as go

# add the project root directory to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(project_root)

from src.streamlit.streamlit_utils import load_data, connect_to_postgres

# load environment variables
load_dotenv()

st.set_page_config(layout="wide")

# connect to the database
connection = connect_to_postgres(
    database=os.getenv("PG_DATABASE"),
    host=os.getenv("PG_HOST"),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD"),
    port=os.getenv("PG_PORT")
)

st.title("Fantasy Premier League Dashboard")


if connection:

    data, column_names = load_data(connection, "dbt_ohempel", "fact_player_performance")


    # Define the schema based on your knowledge of the data types in each column
    schema = {
        # "player_performance_id": pl.Int64,
        "player_name": pl.Utf8,
        "season": pl.Utf8,
        "gameweek": pl.Int64,
        "team": pl.Utf8,
        "opponent_team": pl.Utf8,
        "position": pl.Utf8,
        "player_cost": pl.Float64,
        "total_points": pl.Int64,
        # "date": pl.Datetime,
        # "team_a_score": pl.Int64,
        # "team_h_score": pl.Int64,
        # "was_home": pl.Boolean,
        "goals_scored": pl.Int64,
        "assists": pl.Int64,
        # "bonus": pl.Int64,
        # "bps": pl.Int64,
        "clean_sheets": pl.Boolean,
        # "creativity": pl.Float64,
        # "element": pl.Int64,
        # "xP": pl.Float64,
        # "expected_assists": pl.Float64,
        # "expected_goal_involvements": pl.Float64,
        # "expected_goals": pl.Float64,
        # "expected_goals_conceded": pl.Float64,
        # "goals_conceded": pl.Int64,
        "ict_index": pl.Float64,
        # "influence": pl.Float64,
        "minutes_played": pl.Int64,
        "kickoff_time": pl.Datetime,
        # "own_goals": pl.Int64,
        # "penalties_missed": pl.Int64,
        # "penalties_saved": pl.Int64,
        # "red_cards": pl.Int64,
        # "saves": pl.Int64,
        # "player_started": pl.Boolean,
        # "threat": pl.Float64,
        # "transfers_balance": pl.Int64,
        # "transfers_in": pl.Int64,
        # "transfers_out": pl.Int64,
        "selected": pl.Int64,
        # "yellow_cards": pl.Int64,
        # "player_id": pl.Utf8,
        # "team_id": pl.Utf8,
        # "fixture_id": pl.Int64,
        # "gameweek_id": pl.Utf8,
        # "season_id": pl.Utf8,
        # "date_id": pl.Utf8,
        # "seasonal_fixture_id": pl.Int64
    }

    # create a polars DataFrame from the data
    df = pl.DataFrame(
        data,
        schema=schema,
        orient="row",
        #infer_schema_length=5000
    )

    # convert kickoff_time column to datetime
    df = df.with_columns(
        pl.col("kickoff_time").cast(pl.Datetime).alias("kickoff_time")
    )

    # Function to get top players based on current filters
    def get_top_players(df, n=5):
        return (
            df.group_by("player_name")
            .agg(pl.sum("total_points").alias("total_points"))
            .sort("total_points", descending=True)
            .head(n)
        )

    # Function to update filters for a given season
    def update_filters_for_season(season):
        season_data = df.filter(pl.col("season") == season)
        st.session_state.selected_teams = season_data["team"].unique().sort().to_list()
        st.session_state.selected_positions = season_data["position"].unique().sort().to_list()
        
        # Update top players for comparison
        top_players = get_top_players(season_data)
        st.session_state.selected_players_for_comparison = top_players["player_name"].to_list()

    # Function to set filters_changed when season changes
    def on_season_change():
        st.session_state.filters_changed = True
        update_filters_for_season(st.session_state.selected_season)

    # Function to set filters_changed when teams change
    def on_team_change():
        st.session_state.filters_changed = True

    # Function to set filters_changed when positions change
    def on_position_change():
        st.session_state.filters_changed = True

    # Function to reset filters
    def reset_filters():
        st.session_state.selected_season = latest_season
        update_filters_for_season(latest_season)
        st.session_state.filters_changed = True
        st.session_state.selected_players_for_comparison = []
        if "selected_teams" in st.session_state:
            del st.session_state.selected_teams
        if "selected_positions" in st.session_state:
            del st.session_state.selected_positions

    # Initialize filters_changed in session state
    if "filters_changed" not in st.session_state:
        st.session_state.filters_changed = True  # Set to True initially to trigger top players selection

    # Get all seasons and determine the latest season
    all_seasons = df["season"].unique().sort(descending=True)
    latest_season = all_seasons[0]

    # Season selection with on_change callback
    selected_season = st.sidebar.selectbox(
        "Select Season", all_seasons, key="selected_season", on_change=on_season_change
    )

    # Update filters when season changes or on initial load
    if "previous_season" not in st.session_state or st.session_state.previous_season != selected_season:
        update_filters_for_season(selected_season)
        st.session_state.previous_season = selected_season

    # Filter data for the selected season
    df_selected_season = df.filter(pl.col("season") == selected_season)

    if df_selected_season.is_empty():
        st.warning(f"No data available for the selected season: {selected_season}")
    else:
        # Team selection with on_change callback
        all_teams = df_selected_season["team"].unique().sort()
        if "selected_teams" not in st.session_state:
            st.session_state.selected_teams = all_teams.to_list()
        selected_teams = st.sidebar.multiselect(
            "Select Teams",
            options=all_teams,
            default=None,
            key="selected_teams",
            on_change=on_team_change
        )

        # Use the session state value if the multiselect is empty
        if not selected_teams:
            selected_teams = st.session_state.selected_teams

        # Position selection with on_change callback
        all_positions = df_selected_season["position"].unique().sort()
        if "selected_positions" not in st.session_state:
            st.session_state.selected_positions = all_positions.to_list()
        selected_positions = st.sidebar.multiselect(
            "Select Positions",
            options=all_positions,
            default=None,
            key="selected_positions",
            on_change=on_position_change
        )

        # Use the session state value if the multiselect is empty
        if not selected_positions:
            selected_positions = st.session_state.selected_positions

        # Filter data based on user selection
        filtered_df = df_selected_season.filter(
            (pl.col("team").is_in(selected_teams)) & 
            (pl.col("position").is_in(selected_positions))
        )

        # Display current filters
        st.sidebar.write(f"Current Season: {selected_season}")
        st.sidebar.write(f"Selected Teams: {', '.join(selected_teams)}")
        st.sidebar.write(f"Selected Positions: {', '.join(selected_positions)}")

        if filtered_df.is_empty():
            st.warning("No data available for the current selection. Please adjust your filters.")
        else:
            # latest gameweek
            latest_gameweek = filtered_df.select(pl.col("gameweek").max()).item()
            st.write(f"Latest Gameweek: {latest_gameweek}")

            # latest kickoff time
            latest_kickoff_time = filtered_df.select(pl.col("kickoff_time").max()).item()

            if latest_kickoff_time is not None:
                st.write(f"Latest Kickoff Time: {latest_kickoff_time}")
                
                # if latest kickoff time is more than 1 week ago, print a message regarding the data freshness
                if latest_kickoff_time < datetime.now() - timedelta(days=14):
                    st.warning("Data is more than 2 weeks old. Owner needs to update the data.")
            else:
                st.write("No kickoff time data available for the selected season.")

            # function to create player chart
            def create_player_chart(position: str, title: str):
                top_players = (
                    filtered_df.filter(pl.col("position") == position)
                    .group_by("player_name")
                    .agg(pl.sum("total_points"))
                    .sort("total_points", descending=True)
                    .head(10)
                )
                
                fig = px.bar(
                    top_players,
                    x="player_name",
                    y="total_points",
                    title=title,
                    labels={"player_name": "Player", "total_points": "Total Points"},
                    text="total_points"
                )
                fig.update_traces(texttemplate="%{text:.0f}", textposition="outside")
                fig.update_layout(
                    xaxis_title="Player",
                    yaxis_title="Total Points",
                    xaxis_tickangle=45,
                    uniformtext_minsize=8,
                    uniformtext_mode="hide",
                    height=500,  # Increase the height of the chart
                    margin=dict(t=50, b=100)  # Adjust margins to accommodate labels
                )
                return fig

            # create and display charts for each position
            for position, title in [
                ("DEF", "Top 10 Defenders"),
                ("MID", "Top 10 Midfielders"),
                ("FWD", "Top 10 Forwards"),
                ("GK", "Top 10 Goalkeepers")
            ]:
                if position in selected_positions:
                    st.plotly_chart(create_player_chart(position, title), use_container_width=True)

            # teams chart
            teams_points = (
                filtered_df.group_by("team")
                .agg(pl.sum("total_points"))
                .sort("total_points", descending=True)
            )
            fig_teams_points = px.bar(
                teams_points,
                x="team",
                y="total_points",
                title="Teams Total Points",
                labels={"team": "Team", "total_points": "Total Points"},
                text="total_points"
            )
            fig_teams_points.update_traces(texttemplate="%{text:.0f}", textposition="outside")
            fig_teams_points.update_layout(
                xaxis_title="Team",
                yaxis_title="Total Points",
                xaxis_tickangle=45,
                uniformtext_minsize=8,
                uniformtext_mode="hide",
                height=500,  # Increase the height of the chart
                margin=dict(t=50, b=100)  # Adjust margins to accommodate labels
            )
            st.plotly_chart(fig_teams_points, use_container_width=True)

            # player comparison
            st.header("Player Comparison")

            # Get available players based on filtered data
            all_players = filtered_df["player_name"].unique().sort()

            # Ensure selected players are valid for current filters
            st.session_state.selected_players_for_comparison = [
                player for player in st.session_state.selected_players_for_comparison if player in all_players
            ]

            # If filters changed and no valid players selected, select top players
            if st.session_state.filters_changed or not st.session_state.selected_players_for_comparison:
                top_players = get_top_players(filtered_df)
                st.session_state.selected_players_for_comparison = top_players["player_name"].to_list()
                st.session_state.filters_changed = False  # Reset the flag

            # Multiselect with default to show top players
            selected_players = st.multiselect(
                "Select Players to Compare",
                options=all_players,
                default=st.session_state.selected_players_for_comparison,
                key="selected_players_for_comparison"
            )

            if selected_players:
                player_data = (
                    filtered_df.filter(pl.col("player_name").is_in(selected_players))
                    .group_by("player_name")
                    .agg([
                        pl.sum("total_points").alias("Total Points"),
                        pl.sum("goals_scored").alias("Goals"),
                        pl.sum("assists").alias("Assists"),
                        pl.mean("player_cost").alias("Avg Cost").round(0),
                        pl.mean("ict_index").alias("ICT Index").round(2),
                        pl.sum("minutes_played").alias("Minutes Played"),
                    ])
                    .sort("Total Points", descending=True)
                )
                
                fig = go.Figure()
                for metric in ["Total Points", "Goals", "Assists", "Avg Cost", "ICT Index"]:
                    fig.add_trace(go.Bar(
                        x=player_data["player_name"],
                        y=player_data[metric],
                        name=metric,
                        text=player_data[metric],
                        textposition="outside"
                    ))
                
                fig.update_layout(
                    title="Player Comparison",
                    xaxis_title="Player",
                    yaxis_title="Value",
                    barmode="group",
                    legend_title="Metric",
                    height=600,  # Increase the height of the chart
                    margin=dict(t=50, b=100)  # Adjust margins to accommodate labels
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("Please select at least one player for comparison.")

    # Add a reset button to the sidebar
    st.sidebar.button("Reset Filters", on_click=reset_filters)

else:
    st.error("Failed to connect to the database")

# TODO Current optimal team based on points with the budget constraint

