import pandas as pd
from pulp import LpMaximize, LpProblem, LpVariable, lpSum, PULP_CBC_CMD

def select_optimal_team(df: pd.DataFrame, budget: float) -> pd.DataFrame:
    # aggregate total points per player across all games
    player_agg = df.groupby(['name', 'position', 'team'], as_index=False).agg(
        total_points=('total_points', 'sum'),
        value=('value', 'mean')  # assuming 'value' is the same across games, so we take the mean
    )
    
    # create the optimization model
    model = LpProblem(name="FPL_Team_Selection", sense=LpMaximize)
    
    # create a list of all players
    players = player_agg.index.tolist()
    
    # define the binary variables indicating whether a player is selected or not
    player_vars = LpVariable.dicts("Player", players, cat="Binary")
    
    # objective: maximize the aggregated total points
    model += lpSum(player_agg['total_points'][i] * player_vars[i] for i in players)
    
    # add constraints based on the budget
    model += lpSum(player_agg['value'][i] * player_vars[i] for i in players) <= budget
    
    # position constraints
    model += lpSum(player_vars[i] for i in players if player_agg['position'][i] == 'GK') == 1  # 1 goalkeeper
    model += lpSum(player_vars[i] for i in players if player_agg['position'][i] == 'DEF') >= 3  # at least 3 defenders
    model += lpSum(player_vars[i] for i in players if player_agg['position'][i] == 'DEF') <= 5  # at most 5 defenders
    model += lpSum(player_vars[i] for i in players if player_agg['position'][i] == 'MID') >= 2  # at least 2 midfielders
    model += lpSum(player_vars[i] for i in players if player_agg['position'][i] == 'MID') <= 5  # at most 5 midfielders
    model += lpSum(player_vars[i] for i in players if player_agg['position'][i] == 'FWD') >= 1  # at least 1 forward
    model += lpSum(player_vars[i] for i in players if player_agg['position'][i] == 'FWD') <= 3  # at most 3 forwards
    
    # team size constraint (exactly 11 players)
    model += lpSum(player_vars[i] for i in players) == 11
    
    # no more than 3 players from a single team
    for team in player_agg['team'].unique():
        model += lpSum(player_vars[i] for i in players if player_agg['team'][i] == team) <= 3
    
    # solve the optimization problem
    solver = PULP_CBC_CMD(timeLimit=600)
    model.solve(solver)
    
    # retrieve the selected players
    selected_players = [i for i in players if player_vars[i].varValue == 1]
    
    # return the data of the selected players
    return player_agg.loc[selected_players]

# Example usage:
# Assuming you have a dataframe `df` with columns:
# - 'name': player name
# - 'position': player's position ('GK', 'DEF', 'MID', 'FWD')
# - 'team': player's team
# - 'total_points': points scored per game per player
# - 'value': player's price per game

# Set your budget, e.g., 100 million
budget = 100.0
optimal_team = select_optimal_team(df, budget)

# print or save the optimal team
print(optimal_team)
