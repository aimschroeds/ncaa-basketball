from venv import create
import mysql.connector
import os
from dotenv import load_dotenv
load_dotenv()
from pathlib import Path
import pickle
from datetime import datetime, timedelta
from read_csv import *

host = os.getenv("DB_HOST")
coder = os.getenv("DB_CODER")
coder_pass = os.getenv("DB_CODER_PASSWORD")

# Connect to mysql database
# play_by_play = mysql.connector.connect(
#     host=host,
#     user=coder,
#     password=coder_pass,
#     database="ncaa_play_by_play"
# )
# mycursor = play_by_play.cursor(buffered=True)

# Get data from csv
data_set = Path(__file__).parent / "../data/data_segmented/2016_split_ac"
rows = read_csv(data_set)

def create_entries(rows):
    # # Delete existing rows in rebounds
    # mycursor.execute("SET foreign_key_checks = 0")
    # mycursor.execute("DELETE FROM rebounds")
    # play_by_play.commit()
    # print(mycursor.rowcount, "rebounds deleted")
    # # Delete existing rows in shots
    # mycursor.execute("DELETE FROM shots")
    # play_by_play.commit()
    # print(mycursor.rowcount, "shots deleted")
    # # Delete existing rows in timeouts
    # mycursor.execute("DELETE FROM timeouts")
    # play_by_play.commit()
    # print(mycursor.rowcount, "timeouts deleted")
    # # Delete existing rows in turnovers
    # mycursor.execute("DELETE FROM turnovers")
    # play_by_play.commit()
    # print(mycursor.rowcount, "turnovers deleted")
    # # Delete existing rows in assists
    # mycursor.execute("DELETE FROM assists")
    # play_by_play.commit()
    # print(mycursor.rowcount, "assists deleted")
    # # Delete existing rows in divisions
    # mycursor.execute("DELETE FROM divisions")
    # play_by_play.commit()
    # print(mycursor.rowcount, "divisions deleted")
    # # Delete existing rows in conferences
    # mycursor.execute("DELETE FROM conferences")
    # play_by_play.commit()
    # print(mycursor.rowcount, "conferences deleted")
    # # Delete existing rows in teams
    # mycursor.execute("DELETE FROM teams")
    # play_by_play.commit()
    # print(mycursor.rowcount, "teams deleted")
    # # Delete existing rows in players
    # mycursor.execute("DELETE FROM players")
    # play_by_play.commit()
    # print(mycursor.rowcount, "players deleted")
    # # Delete existing rows in tournaments
    # mycursor.execute("DELETE FROM tournaments")
    # play_by_play.commit()
    # print(mycursor.rowcount, "tournaments deleted")
    # # Delete existing rows in venues
    # mycursor.execute("DELETE FROM venues")
    # play_by_play.commit()
    # print(mycursor.rowcount, "venues deleted")
    # # Delete existing rows in games
    # mycursor.execute("DELETE FROM games")
    # play_by_play.commit()
    # print(mycursor.rowcount, "games deleted")
    # # Delete existing rows in events
    # mycursor.execute("DELETE FROM events")
    # play_by_play.commit()
    # print(mycursor.rowcount, "events deleted")
    # # Delete existing rows in event_types    
    # mycursor.execute("DELETE FROM event_types")
    # play_by_play.commit()
    # print(mycursor.rowcount, "event types deleted")
    # mycursor.execute("SET foreign_key_checks = 1")

    with open('rebounds.txt','rb') as f:
        rebounds = pickle.load(f)
    with open('shots.txt','rb') as f:
        shots = pickle.load(f)
    with open('timeouts.txt','rb') as f:
        timeouts = pickle.load(f)
    with open('turnovers.txt','rb') as f:
        turnovers = pickle.load(f)
    with open('assists.txt','rb') as f:
        assists = pickle.load(f)
    with open('divisions.txt','rb') as f:
        divisions = pickle.load(f)
    with open('conferences.txt','rb') as f:
        conferences = pickle.load(f)
    with open('teams.txt','rb') as f:
        teams = pickle.load(f)

    with open('players.txt','rb') as f:
        players = pickle.load(f)
    with open('games.txt','rb') as f:
        games = pickle.load(f)
    with open('tournaments.txt','rb') as f:
        tournaments = pickle.load(f)
    with open('venues.txt','rb') as f:
        venues = pickle.load(f)

    for row in rows:
        rebounds=populate_rebound(row, rebounds)
        shots=populate_shot(row, shots)
        timeouts=populate_timeout(row, timeouts)
        turnovers=populate_turnover(row, turnovers)
        assists=populate_assist(row, assists)
    
        divisions=populate_division(row, divisions)
        conferences=populate_conference(row, conferences)
        teams=populate_team(row, teams)
        players = populate_player(row, players)
        tournaments = populate_tournament(row, tournaments)
        venues = populate_venue(row, venues)
        games=populate_game(row, games)
        # populate_event(row)
    
    ##To save in file
    with open('rebounds.txt','wb') as f:
        pickle.dump(rebounds, f)
    with open('shots.txt','wb') as f:
        pickle.dump(shots, f)
    with open('turnovers.txt','wb') as f:
        pickle.dump(turnovers, f)
    with open('timeouts.txt','wb') as f:
        pickle.dump(timeouts, f)
    with open('assists.txt','wb') as f:
        pickle.dump(assists, f)
    with open('divisions.txt','wb') as f:
        pickle.dump(divisions, f)
    with open('conferences.txt','wb') as f:
        pickle.dump(conferences, f)
    with open('teams.txt','wb') as f:
        pickle.dump(teams, f)
    with open('players.txt','wb') as f:
        pickle.dump(players, f)
    with open('tournaments.txt','wb') as f:
        pickle.dump(tournaments, f)
    with open('venues.txt','wb') as f:
        pickle.dump(venues, f)
    with open('games.txt','wb') as f:
        pickle.dump(games, f)

# Populate rebounds table
def populate_rebound(row, rebound_types):
    '''Takes in a row; populates the rebounds table'''
    if row['event_type'] == 'rebound':
        if (row['rebound_type'] not in rebound_types):
            # mycursor.execute("INSERT INTO rebounds (type) VALUES (%s)", (row['rebound_type'],))
            rebound_types.add(row['rebound_type'])
            # play_by_play.commit()
            print("Rebound added")
    else:
        # print("No rebounds added")
        pass
    return rebound_types

# Populate shots table
def populate_shot(row, shots):
    '''Takes in a list of lists; populates the shots table'''
    # Identify rows that are shots
    if row['type'] == 'fieldgoal' or row['type'] == 'freethrow':
        if (row['type'], row['shot_type'], row['shot_subtype'], row['points_scored'], row['shot_made']) not in shots:
            # Clean data
            shot_made = True if row['shot_made'] == 'true' else (False if row['shot_made'] == 'false' else None)
            points = int(row['points_scored']) if row['points_scored'] else None
            # Insert row
            # mycursor.execute("INSERT INTO shots (type, sub_type, basket_type, points, made) VALUES (%s, %s, %s, %s, %s)", (row['type'], row['shot_type'], row['shot_subtype'], points, shot_made))
            shots.add((row['type'], row['shot_type'], row['shot_subtype'], row['points_scored'], row['shot_made']))
            # play_by_play.commit()
            print("Shot added")
        else:
            # print("No shots added")
            pass
    return shots

# Populate timeouts table
def populate_timeout(row, timeouts):
    '''Takes in a list of lists; populates the timeouts table'''
    # Identify rows that are timeouts
    event_type = str(row['event_type'])
    if 'timeout' in event_type:
        if event_type not in timeouts:
            # Insert row
            print(event_type)
            # mycursor.execute("INSERT INTO timeouts (type) VALUES (%s)", (event_type,))
            timeouts.add(event_type)
            # play_by_play.commit()
            print("Timeout added")
        else:
            # print("No timeouts added")
            pass
    return timeouts


# Populate turnovers table
def populate_turnover(row, turnovers):
    '''Takes in a list of lists; populates the turnovers table'''
    # Identify rows that are turnovers
    event_type = str(row['event_type'])
    if event_type == 'turnover':
        if str(row['type']) not in turnovers:
            # Insert row
            # mycursor.execute("INSERT INTO turnovers (type) VALUES (%s)", (str(row['type']),))
            turnovers.add(str(row['type']))
            # play_by_play.commit()
            print("Turnover added")
        else:
            # print("No turnovers added")
            pass
    return turnovers

# Populate assists table
def populate_assist(row, assists):
    '''Takes in a list of lists; populates the assists table'''
    # Identify rows that are turnovers
    event_type = str(row['event_type'])
    if event_type == 'assist':
        if str(row['type']) not in assists:
            # Insert row
            # mycursor.execute("INSERT INTO assists (type) VALUES (%s)", (str(row['type']),))
            assists.add(str(row['type']))
            # play_by_play.commit()
            print("Assist added")
        else:
            # print("No assists added")
            pass
    return assists

# Populate divisions table
def populate_division(row, divisions):
    '''Takes in a list of lists; populates the divisions table'''
    
    # Add division if not yet in table
    if row['away_division_name'] not in divisions:
        # Insert row
        print(row['away_division_name'])
        # mycursor.execute("INSERT INTO divisions (name, alias) VALUES (%s, %s)", (row['away_division_name'], row['away_division_alias']))
        divisions.add(row['away_division_name'])
        # play_by_play.commit()
        print("Division added")

    if row['home_division_name'] not in divisions:
        # Insert row
        print(row['home_division_name'])
        # mycursor.execute("INSERT INTO divisions (name, alias) VALUES (%s, %s)", (row['home_division_name'], row['home_division_alias']))
        divisions.add(row['home_division_name'])
        # play_by_play.commit()
        print("Division added")
    else:
        # print("No divisions added")
        pass
    return divisions

# Populate conferences table
def populate_conference(row, conferences):
    '''Takes in a list of lists; populates the conferences table'''
    # Add conference if not yet in table
    if row['away_conf_name'] not in conferences:
        # Insert row      
        div_name = row['away_division_name'].strip()      
        # mycursor.execute("SELECT id FROM divisions WHERE name=%s", (div_name,))
        # division_id = mycursor.fetchone()[0]
        # mycursor.execute("INSERT INTO conferences (name, alias, division_id) VALUES (%s, %s, %s)", (row['away_conf_name'], row['away_conf_alias'], division_id))
        conferences.add(row['away_conf_name'])
        # play_by_play.commit()
        print("Conference added")

    if row['home_conf_name'] not in conferences:
        # Insert row
        div_name = row['away_division_name'].strip()
        # mycursor.execute("SELECT id FROM divisions WHERE name=%s", (div_name,))
        # division_id = mycursor.fetchone()[0]
        # mycursor.execute("INSERT INTO conferences (name, alias, division_id) VALUES (%s, %s, %s)", (row['home_conf_name'], row['home_conf_alias'], division_id))
        conferences.add(row['home_conf_name'])
        # play_by_play.commit()
        print("Conference added")
    else:
        # print("No conferences added")
        pass
    return conferences

# Populate teams table
def populate_team(row, teams):
    '''Takes in a list of lists; populates the teams table'''
    # Add team if not yet in table
    if (row['away_name'], row['away_market']) not in teams:
        # Insert row
        # print(row['away_name'])
        # mycursor.execute("SELECT id FROM conferences WHERE name=%s", (row['away_conf_name'],))
        # conference_id = mycursor.fetchone()[0]
        # mycursor.execute("INSERT INTO teams (name, alias, market, conference_id) VALUES (%s, %s, %s, %s)", (row['away_name'], row['away_alias'], row['away_market'], conference_id))
        teams.add((row['away_name'], row['away_market']))
        # play_by_play.commit()
        # print("Team added")

    if (row['home_name'], row['home_market']) not in teams:
        # Insert row
        # print(row['home_name'])
        # mycursor.execute("SELECT id FROM conferences WHERE name=%s", (row['home_conf_name'],))
        # conference_id = mycursor.fetchone()[0]
        # mycursor.execute("INSERT INTO teams (name, alias, market, conference_id) VALUES (%s, %s, %s, %s)", (row['home_name'], row['home_alias'], row['home_market'], conference_id))
        teams.add((row['home_name'], row['home_market']))
        # play_by_play.commit()
        # print("Team added")
    else:
        # print("No teams added")
        # print(row['home_name'])
        # print(row['away_name'])
        pass
    return teams


# Populate players table
def populate_player(row, players):
    '''Takes in a list of lists; populates the players table'''
    # Add player if not yet in table
    if (row['player_full_name'], row['jersey_num']) not in players and row['player_full_name'] is not None:
        # Insert row
        jersey_number = int(row['jersey_num']) if row['jersey_num'] else None
        if (row['team_name']) is not None and (row['team_market']) is not None:
            # mycursor.execute("SELECT id FROM teams WHERE name=%s AND market=%s", (row['team_name'], row['team_market']))
            # mycursor.execute("SELECT id FROM teams WHERE name=%s AND market=%s", (row['team_name'], row['team_market']))
            print(row['team_name'])
            print(row['team_market'])
            # if mycursor.rowcount == 0:
                # print("No team found")
            # else:
                # team_id = mycursor.fetchone()[0]
                # print(team_id)
                # mycursor.execute("INSERT INTO players (name, jersey_number, team_id) VALUES (%s, %s, %s)", (row['player_full_name'], jersey_number, team_id))
            players.add((row['player_full_name'], row['jersey_num']) )
                # play_by_play.commit()
            print("Player added")
    else:
        # print("No players added")
        pass
    return players


# Populate tournaments table
def populate_tournament(row, tournaments):
    '''Takes in a list of lists; populates the tournaments table'''

    # Add tournament if not yet in table
    if ((row['tournament'], row['tournament_type'])) not in tournaments:
        # Insert row
        print(row['tournament'])
        # mycursor.execute("INSERT INTO tournaments (name, type) VALUES (%s, %s)", (row['tournament'], row['tournament_type']))
        tournaments.add((row['tournament'], row['tournament_type']))
        # play_by_play.commit()
        print("Tournament added")
    else:
        # print("No tournaments added")
        pass
    return tournaments


# Populate venues table
def populate_venue(row, venues):
    '''Takes in a list of lists; populates the venues table'''
    # Add venue if not yet in table
    if (row['venue_name']) not in venues:
        # Insert row
        print(row['venue_name'])
        # mycursor.execute("INSERT INTO venues (name, city, state, address, zip, country_code, capacity) VALUES (%s, %s, %s, %s, %s, %s, %s)", (row['venue_name'],row['venue_city'],row['venue_state'],row['venue_address'],row['venue_zip'],row['venue_country'], row['venue_capacity']))
        venues.add(row['venue_name'])
        # play_by_play.commit()
        print("Venue added")
    else:
        # print("No venues added")
        pass
    return venues


# Populate games table
def populate_game(row, games):
    '''Takes in a list of lists; populates the games table'''
    # Add game if not yet in table
    if ((row['home_name'], row['home_market'], row['away_name'], row['away_market'], row['scheduled_date'])) not in games:
        # Clean data
        neutral_site = True if row['neutral_site'] == 'true' else (False if row['neutral_site'] == 'false' else None)
        conference_game = True if row['conference_game'] == 'true' else (False if row['conference_game'] == 'false' else None)
        attendance = int(row['attendance']) if row['attendance'] else None
        # Get foreign key IDs
        # mycursor.execute("SELECT id FROM teams WHERE name=%s AND market=%s", (row['home_name'], row['home_market']))
        # home_team_id = None if mycursor.rowcount == 0 else mycursor.fetchone()[0]
        # mycursor.execute("SELECT id FROM teams WHERE name=%s AND market=%s", (row['away_name'], row['away_market']))
        # away_team_id = None if mycursor.rowcount == 0 else mycursor.fetchone()[0]
        # mycursor.execute("SELECT id FROM tournaments WHERE name=%s AND type=%s", (row['tournament'], row['tournament_type']))
        # tournament_id = None if mycursor.rowcount == 0 else mycursor.fetchone()[0]
        # mycursor.execute("SELECT id FROM venues WHERE name =%s", (row['venue_name'],))
        # venue_id = None if mycursor.rowcount == 0 else mycursor.fetchone()[0]
        # # Insert row
        # mycursor.execute("INSERT INTO games (home_team_id, away_team_id, tournament_id, venue_id, game_no, round, conference_game, neutral_site, attendance,  season, scheduled_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (home_team_id, away_team_id, tournament_id, venue_id, row['game_no'], row['round'], conference_game, neutral_site, attendance, row['season'], row['scheduled_date'][:-3]))
        games.add((row['home_name'], row['home_market'], row['away_name'], row['away_market'], row['scheduled_date']))
        # play_by_play.commit()
        print("Game added")
    else:
        # print("No games added")
        pass
    return games


# Populate events table
def populate_event(row):
    '''Takes in a list of lists; populates the events table'''
       
    game_id_query = """
        SELECT game_id.id FROM (
        SELECT games.id, 
        scheduled_date,
        home_team_id,
        away_team_id,
        home.name AS home_team_name,
        home.market AS home_team_market,
        away.name AS away_team_name,
        away.market AS away_team_market
        FROM games
        LEFT JOIN teams home ON games.home_team_id = home.id
        LEFT JOIN teams away ON games.away_team_id = away.id
        WHERE scheduled_date=%s
        AND home.name=%s
        AND home.market=%s
        AND away.name=%s
        AND away.market=%s) AS game_id"""
    mycursor.execute(game_id_query, (row['scheduled_date'][:-3], row['home_name'], row['home_market'], row['away_name'], row['away_market']))
    game_id = mycursor.fetchone()[0]
    # print(game_id)
    mycursor.execute("SELECT id FROM teams WHERE name=%s AND market=%s", (row['team_name'], row['team_market']))
    team_id = None if mycursor.rowcount == 0 else mycursor.fetchone()[0]
    mycursor.execute("SELECT id FROM players WHERE name=%s AND jersey_number=%s AND team_id=%s", (row['player_full_name'], row['jersey_num'], team_id))
    player_id = None if mycursor.rowcount == 0 else mycursor.fetchone()[0]
    coord_x = int(row['event_coord_x']) if row['event_coord_x'] else None
    coord_y = int(row['event_coord_y']) if row['event_coord_y'] else None
    if (row['possession_team_id'] == row['home_id']):
        possession_team_name = row['home_name']
        possession_team_market = row['home_market']
    elif (row['possession_team_id'] == row['away_id']):
        possession_team_name = row['away_name']
        possession_team_market = row['away_market']
    else:
        possession_team_name = None
    if possession_team_name is not None:
        mycursor.execute("SELECT id FROM teams WHERE name=%s AND market=%s", (possession_team_name, possession_team_market))
        possession_team_id = None if mycursor.rowcount == 0 else mycursor.fetchone()[0]
    else:
        possession_team_id = None
    # Insert row into events table
    # Considering date is in YYYY/mm/dd HH:MM:SS format
    timestamp = datetime.strptime(row['timestamp'], "%Y-%m-%d %H:%M:%S %Z")
    if timestamp > datetime.strptime("2017-03-26 02:00:00", "%Y-%m-%d %H:%M:%S") and timestamp < datetime.strptime("2017-03-26 03:00:00", "%Y-%m-%d %H:%M:%S"):
        timestamp = timestamp - timedelta(hours=1)
    
    mycursor.execute("INSERT INTO events (game_id, period, game_clock, elapsed_time_seconds, team_id, player_id, timestamp, description, coord_x, coord_y, team_with_possession_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (game_id, row['period'], row['game_clock'], row['elapsed_time_sec'],team_id, player_id, timestamp, row['event_description'], coord_x, coord_y, possession_team_id))
    play_by_play.commit()

    event_id = mycursor.lastrowid
    event_type = row['event_type']
    type = row['type']
    if event_type == 'rebound':
        mycursor.execute("SELECT id FROM rebounds WHERE type=%s", (row['rebound_type'],))
        rebound_id = None if mycursor.rowcount == 0 else mycursor.fetchone()[0]
    else: rebound_id = None
    if event_type == 'assist':
        mycursor.execute("SELECT id FROM assists WHERE type=%s", (row['type'],))
        assist_id = None if mycursor.rowcount == 0 else mycursor.fetchone()[0]
    else: assist_id = None
    if event_type == 'turnover':
        mycursor.execute("SELECT id FROM turnovers WHERE type=%s", (row['turnover_type'],))
        turnover_id = None if mycursor.rowcount == 0 else mycursor.fetchone()[0]
    else: turnover_id = None
    if type == 'fieldgoal' or type == 'freethrow':
        # Clean data
        shot_made = True if row['shot_made'] == 'true' else (False if row['shot_made'] == 'false' else None)
        points = int(row['points_scored']) if row['points_scored'] else None
        mycursor.execute("SELECT id FROM shots WHERE type=%s AND sub_type=%s AND basket_type=%s AND points=%s AND made=%s", (row['type'], row['shot_type'], row['shot_subtype'], points, shot_made))
        shot_id = None if mycursor.rowcount == 0 else mycursor.fetchone()[0]
    else: shot_id = None
    if 'timeout' in str(event_type):
        mycursor.execute("SELECT id FROM timeouts WHERE type=%s", (event_type,))
        timeout_id = None if mycursor.rowcount == 0 else mycursor.fetchone()[0]
    else: timeout_id = None

    mycursor.execute("INSERT INTO event_types (event_id, type, rebound_id, shot_id, turnover_id, assist_id, timeout_id) VALUES (%s, %s, %s, %s, %s, %s, %s)", (event_id, event_type, rebound_id, shot_id, turnover_id, assist_id, timeout_id))
    play_by_play.commit()

# Populate event types table
def populate_event_type(row):
    '''Takes in a list of lists; populates the event types table'''
    # Delete existing rows in table
    mycursor.execute("SET foreign_key_checks = 0")
    mycursor.execute("DELETE FROM event_types")
    play_by_play.commit()
    mycursor.execute("SET foreign_key_checks = 1")
    print(mycursor.rowcount, "event types deleted")

    event_types = set()
    for row in rows:
        # Add event type if not yet in table
        if (row['event_type']) not in event_types:
            # Insert row
            print(row['event_type'])
            mycursor.execute("INSERT INTO event_types (name) VALUES (%s)", (row['event_type'],))
            event_types.add(row['event_type'])
            play_by_play.commit()
            print("Event type added")
        else:
            # print("No event types added")
            pass
    return rows

# Populate rebounds table
def populate_rebounds(rows):
    '''Takes in a list of lists; populates the rebounds table'''
    # Delete existing rows in table
    mycursor.execute("DELETE FROM rebounds")
    play_by_play.commit()
    print(mycursor.rowcount, "rebounds deleted")

    rebound_types = set()
    for row in rows:
        if row['event_type'] == 'rebound':
            if (row['rebound_type'] not in rebound_types):
                mycursor.execute("INSERT INTO rebounds (type) VALUES (%s)", (row['rebound_type'],))
                rebound_types.add(row['rebound_type'])
                play_by_play.commit()
                print("Rebound added")
        else:
            # print("No rebounds added")
            pass
    return rows

# Populate shots table
def populate_shots(rows):
    '''Takes in a list of lists; populates the shots table'''
    # Delete existing rows in table
    mycursor.execute("DELETE FROM shots")
    play_by_play.commit()
    print(mycursor.rowcount, "shots deleted")

    shots = set()
    for row in rows:
        # Identify rows that are shots
        if row['type'] == 'fieldgoal' or row['type'] == 'freethrow':
            if (row['type'], row['shot_type'], row['shot_subtype'], row['points_scored'], row['shot_made']) not in shots:
                # Clean data
                shot_made = True if row['shot_made'] == 'true' else (False if row['shot_made'] == 'false' else None)
                points = int(row['points_scored']) if row['points_scored'] else None
                # Insert row
                mycursor.execute("INSERT INTO shots (type, sub_type, basket_type, points, made) VALUES (%s, %s, %s, %s, %s)", (row['type'], row['shot_type'], row['shot_subtype'], points, shot_made))
                shots.add((row['type'], row['shot_type'], row['shot_subtype'], row['points_scored'], row['shot_made']))
                play_by_play.commit()
                print("Shot added")
            else:
                # print("No shots added")
                pass
    return rows

# Populate timeouts table
def populate_timeouts(rows):
    '''Takes in a list of lists; populates the timeouts table'''
    # Delete existing rows in table
    mycursor.execute("DELETE FROM timeouts")
    play_by_play.commit()
    print(mycursor.rowcount, "timeouts deleted")

    timeouts = set()
    for row in rows:
        # Identify rows that are timeouts
        event_type = str(row['event_type'])
        if 'timeout' in event_type:
            if event_type not in timeouts:
                # Insert row
                print(event_type)
                mycursor.execute("INSERT INTO timeouts (type) VALUES (%s)", (event_type,))
                timeouts.add(event_type)
                play_by_play.commit()
                print("Timeout added")
            else:
                # print("No timeouts added")
                pass
    return rows


# Populate turnovers table
def populate_turnovers(rows):
    '''Takes in a list of lists; populates the turnovers table'''
    # Delete existing rows in table
    mycursor.execute("DELETE FROM turnovers")
    play_by_play.commit()
    print(mycursor.rowcount, "turnovers deleted")

    turnovers = set()
    for row in rows:
        # Identify rows that are turnovers
        event_type = str(row['event_type'])
        if event_type == 'turnover':
            if str(row['type']) not in turnovers:
                # Insert row
                mycursor.execute("INSERT INTO turnovers (type) VALUES (%s)", (str(row['type']),))
                turnovers.add(str(row['type']))
                play_by_play.commit()
                print("Turnover added")
            else:
                # print("No turnovers added")
                pass
    return rows

# Populate assists table
def populate_assists(rows):
    '''Takes in a list of lists; populates the assists table'''
    # Delete existing rows in table
    mycursor.execute("DELETE FROM assists")
    play_by_play.commit()
    print(mycursor.rowcount, "assists deleted")

    assists = set()
    for row in rows:
        # Identify rows that are turnovers
        event_type = str(row['event_type'])
        if event_type == 'assist':
            if str(row['type']) not in assists:
                # Insert row
                mycursor.execute("INSERT INTO assists (type) VALUES (%s)", (str(row['type']),))
                assists.add(str(row['type']))
                play_by_play.commit()
                print("Assist added")
            else:
                # print("No assists added")
                pass
    return rows

# Populate divisions table
def populate_divisions(rows):
    '''Takes in a list of lists; populates the divisions table'''
    # Delete existing rows in table
    mycursor.execute("SET foreign_key_checks = 0")
    mycursor.execute("DELETE FROM divisions")
    play_by_play.commit()
    mycursor.execute("SET foreign_key_checks = 1")
    print(mycursor.rowcount, "divisions deleted")

    divisions = set()
    for row in rows:
        # Add division if not yet in table
        if row['away_division_name'] not in divisions:
            # Insert row
            print(row['away_division_name'])
            mycursor.execute("INSERT INTO divisions (name, alias) VALUES (%s, %s)", (row['away_division_name'], row['away_division_alias']))
            divisions.add(row['away_division_name'])
            play_by_play.commit()
            print("Division added")

        if row['home_division_name'] not in divisions:
            # Insert row
            print(row['home_division_name'])
            mycursor.execute("INSERT INTO divisions (name, alias) VALUES (%s, %s)", (row['home_division_name'], row['home_division_alias']))
            divisions.add(row['home_division_name'])
            play_by_play.commit()
            print("Division added")
        else:
            # print("No divisions added")
            pass
    return rows

# Populate conferences table
def populate_conferences(rows):
    '''Takes in a list of lists; populates the conferences table'''
    # Delete existing rows in table
    mycursor.execute("SET foreign_key_checks = 0")
    mycursor.execute("DELETE FROM conferences")
    play_by_play.commit()
    mycursor.execute("SET foreign_key_checks = 1")
    print(mycursor.rowcount, "conferences deleted")

    conferences = set()
    for row in rows:
        # Add conference if not yet in table
        if row['away_conf_name'] not in conferences:
            # Insert row      
            div_name = row['away_division_name'].strip()      
            mycursor.execute("SELECT id FROM divisions WHERE name=%s", (div_name,))
            division_id = mycursor.fetchone()[0]
            mycursor.execute("INSERT INTO conferences (name, alias, division_id) VALUES (%s, %s, %s)", (row['away_conf_name'], row['away_conf_alias'], division_id))
            conferences.add(row['away_conf_name'])
            play_by_play.commit()
            print("Conference added")

        if row['home_conf_name'] not in conferences:
            # Insert row
            div_name = row['away_division_name'].strip()
            mycursor.execute("SELECT id FROM divisions WHERE name=%s", (div_name,))
            division_id = mycursor.fetchone()[0]
            mycursor.execute("INSERT INTO conferences (name, alias, division_id) VALUES (%s, %s, %s)", (row['home_conf_name'], row['home_conf_alias'], division_id))
            conferences.add(row['home_conf_name'])
            play_by_play.commit()
            print("Conference added")
        else:
            # print("No conferences added")
            pass
    return rows

# Populate teams table
def populate_teams(rows):
    '''Takes in a list of lists; populates the teams table'''
    # Delete existing rows in table
    mycursor.execute("SET foreign_key_checks = 0")
    mycursor.execute("DELETE FROM teams")
    play_by_play.commit()
    mycursor.execute("SET foreign_key_checks = 1")
    print(mycursor.rowcount, "teams deleted")
    
    teams = set()
    for row in rows:
        if (row['home_market']=='LSU'):
            print("LSU exists")
        # Add team if not yet in table
        if (row['away_name'], row['away_market']) not in teams:
            # Insert row
            # print(row['away_name'])
            mycursor.execute("SELECT id FROM conferences WHERE name=%s", (row['away_conf_name'],))
            conference_id = mycursor.fetchone()[0]
            mycursor.execute("INSERT INTO teams (name, alias, market, conference_id) VALUES (%s, %s, %s, %s)", (row['away_name'], row['away_alias'], row['away_market'], conference_id))
            teams.add((row['away_name'], row['away_market']))
            play_by_play.commit()
            # print("Team added")

        if (row['home_name'], row['home_market']) not in teams:
            # Insert row
            # print(row['home_name'])
            mycursor.execute("SELECT id FROM conferences WHERE name=%s", (row['home_conf_name'],))
            conference_id = mycursor.fetchone()[0]
            mycursor.execute("INSERT INTO teams (name, alias, market, conference_id) VALUES (%s, %s, %s, %s)", (row['home_name'], row['home_alias'], row['home_market'], conference_id))
            teams.add((row['home_name'], row['home_market']))
            play_by_play.commit()
            # print("Team added")
        else:
            print("No teams added")
            # print(row['home_name'])
            # print(row['away_name'])
            pass
    return rows


# Populate players table
def populate_players(rows):
    '''Takes in a list of lists; populates the players table'''
    # Delete existing rows in table
    mycursor.execute("SET foreign_key_checks = 0")
    mycursor.execute("DELETE FROM players")
    play_by_play.commit()
    mycursor.execute("SET foreign_key_checks = 1")
    print(mycursor.rowcount, "players deleted")

    players = set()
    for row in rows:
        # Add player if not yet in table
        if (row['player_full_name'], row['jersey_num']) not in players and row['player_full_name'] is not None:
            # Insert row
            jersey_number = int(row['jersey_num']) if row['jersey_num'] else None
            if (row['team_name']) is not None and (row['team_market']) is not None:
                mycursor.execute("SELECT id FROM teams WHERE name=%s AND market=%s", (row['team_name'], row['team_market']))
                # mycursor.execute("SELECT id FROM teams WHERE name=%s AND market=%s", (row['team_name'], row['team_market']))
                print(row['team_name'])
                print(row['team_market'])
                if mycursor.rowcount == 0:
                    print("No team found")
                else:
                    team_id = mycursor.fetchone()[0]
                    print(team_id)
                    mycursor.execute("INSERT INTO players (name, jersey_number, team_id) VALUES (%s, %s, %s)", (row['player_full_name'], jersey_number, team_id))
                    players.add((row['player_full_name'], row['jersey_num']) )
                    play_by_play.commit()
                    print("Player added")
        else:
            # print("No players added")
            pass
    return rows


# Populate tournaments table
def populate_tournaments(rows):
    '''Takes in a list of lists; populates the tournaments table'''
    # Delete existing rows in table
    mycursor.execute("SET foreign_key_checks = 0")
    mycursor.execute("DELETE FROM tournaments")
    play_by_play.commit()
    mycursor.execute("SET foreign_key_checks = 1")
    print(mycursor.rowcount, "tournaments deleted")

    tournaments = set()
    for row in rows:
        # Add tournament if not yet in table
        if ((row['tournament'], row['tournament_type'])) not in tournaments:
            # Insert row
            print(row['tournament'])
            mycursor.execute("INSERT INTO tournaments (name, type) VALUES (%s, %s)", (row['tournament'], row['tournament_type']))
            tournaments.add((row['tournament'], row['tournament_type']))
            play_by_play.commit()
            print("Tournament added")
        else:
            # print("No tournaments added")
            pass
    return rows


# Populate venues table
def populate_venues(rows):
    '''Takes in a list of lists; populates the venues table'''
    # Delete existing rows in table
    mycursor.execute("SET foreign_key_checks = 0")
    mycursor.execute("DELETE FROM venues")
    play_by_play.commit()
    print(mycursor.rowcount, "venues deleted")
    mycursor.execute("SET foreign_key_checks = 1")

    venues = set()
    for row in rows:
        # Add venue if not yet in table
        if (row['venue_name']) not in venues:
            # Insert row
            print(row['venue_name'])
            mycursor.execute("INSERT INTO venues (name, city, state, address, zip, country_code, capacity) VALUES (%s, %s, %s, %s, %s, %s, %s)", (row['venue_name'],row['venue_city'],row['venue_state'],row['venue_address'],row['venue_zip'],row['venue_country'], row['venue_capacity']))
            venues.add(row['venue_name'])
            play_by_play.commit()
            print("Venue added")
        else:
            # print("No venues added")
            pass
    return rows


# Populate games table
def populate_games(rows):
    '''Takes in a list of lists; populates the games table'''
    # Delete existing rows in table
    mycursor.execute("SET foreign_key_checks = 0")
    mycursor.execute("DELETE FROM games")
    play_by_play.commit()
    mycursor.execute("SET foreign_key_checks = 1")
    print(mycursor.rowcount, "games deleted")

    games = set()
    for row in rows:
        # Add game if not yet in table
        if ((row['home_name'], row['home_market'], row['away_name'], row['away_market'], row['scheduled_date'])) not in games:
            # Clean data
            neutral_site = True if row['neutral_site'] == 'true' else (False if row['neutral_site'] == 'false' else None)
            conference_game = True if row['conference_game'] == 'true' else (False if row['conference_game'] == 'false' else None)
            attendance = int(row['attendance']) if row['attendance'] else None
            # Get foreign key IDs
            mycursor.execute("SELECT id FROM teams WHERE name=%s AND market=%s", (row['home_name'], row['home_market']))
            home_team_id = None if mycursor.rowcount == 0 else mycursor.fetchone()[0]
            mycursor.execute("SELECT id FROM teams WHERE name=%s AND market=%s", (row['away_name'], row['away_market']))
            away_team_id = None if mycursor.rowcount == 0 else mycursor.fetchone()[0]
            mycursor.execute("SELECT id FROM tournaments WHERE name=%s AND type=%s", (row['tournament'], row['tournament_type']))
            tournament_id = None if mycursor.rowcount == 0 else mycursor.fetchone()[0]
            mycursor.execute("SELECT id FROM venues WHERE name =%s", (row['venue_name'],))
            venue_id = None if mycursor.rowcount == 0 else mycursor.fetchone()[0]
            # Insert row
            mycursor.execute("INSERT INTO games (home_team_id, away_team_id, tournament_id, venue_id, game_no, round, conference_game, neutral_site, attendance,  season, scheduled_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (home_team_id, away_team_id, tournament_id, venue_id, row['game_no'], row['round'], conference_game, neutral_site, attendance, row['season'], row['scheduled_date'][:-3]))
            games.add((row['home_name'], row['home_market'], row['away_name'], row['away_market'], row['scheduled_date']))
            play_by_play.commit()
            print("Game added")
        else:
            # print("No games added")
            pass
    return rows


# Populate events table
def populate_events(rows):
    '''Takes in a list of lists; populates the events table'''
    # Delete existing rows in table
    mycursor.execute("SET foreign_key_checks = 0")
    mycursor.execute("DELETE FROM events")
    play_by_play.commit()
    print(mycursor.rowcount, "events deleted")

    mycursor.execute("DELETE FROM event_types")
    play_by_play.commit()
    print(mycursor.rowcount, "event types deleted")
    mycursor.execute("SET foreign_key_checks = 1")
    

    events = set()
    for row in rows:
       
        game_id_query = """
            SELECT game_id.id FROM (
            SELECT games.id, 
            scheduled_date,
            home_team_id,
            away_team_id,
            home.name AS home_team_name,
            home.market AS home_team_market,
            away.name AS away_team_name,
            away.market AS away_team_market
            FROM games
            LEFT JOIN teams home ON games.home_team_id = home.id
            LEFT JOIN teams away ON games.away_team_id = away.id
            WHERE scheduled_date=%s
            AND home.name=%s
            AND home.market=%s
            AND away.name=%s
            AND away.market=%s) AS game_id"""
        mycursor.execute(game_id_query, (row['scheduled_date'][:-3], row['home_name'], row['home_market'], row['away_name'], row['away_market']))
        game_id = mycursor.fetchone()[0]
        # print(game_id)
        mycursor.execute("SELECT id FROM teams WHERE name=%s AND market=%s", (row['team_name'], row['team_market']))
        team_id = None if mycursor.rowcount == 0 else mycursor.fetchone()[0]
        mycursor.execute("SELECT id FROM players WHERE name=%s AND jersey_number=%s AND team_id=%s", (row['player_full_name'], row['jersey_num'], team_id))
        player_id = None if mycursor.rowcount == 0 else mycursor.fetchone()[0]
        coord_x = int(row['event_coord_x']) if row['event_coord_x'] else None
        coord_y = int(row['event_coord_y']) if row['event_coord_y'] else None
        if (row['possession_team_id'] == row['home_id']):
            possession_team_name = row['home_name']
            possession_team_market = row['home_market']
        elif (row['possession_team_id'] == row['away_id']):
            possession_team_name = row['away_name']
            possession_team_market = row['away_market']
        else:
            possession_team_name = None
        if possession_team_name is not None:
            mycursor.execute("SELECT id FROM teams WHERE name=%s AND market=%s", (possession_team_name, possession_team_market))
            possession_team_id = None if mycursor.rowcount == 0 else mycursor.fetchone()[0]
        else:
            possession_team_id = None
        # Insert row into events table
        # Considering date is in YYYY/mm/dd HH:MM:SS format
        timestamp = datetime.strptime(row['timestamp'], "%Y-%m-%d %H:%M:%S %Z")
        if timestamp > datetime.strptime("2017-03-26 02:00:00", "%Y-%m-%d %H:%M:%S") and timestamp < datetime.strptime("2017-03-26 03:00:00", "%Y-%m-%d %H:%M:%S"):
            timestamp = timestamp - datetime.timedelta(hours=1)
        
        mycursor.execute("INSERT INTO events (game_id, period, game_clock, elapsed_time_seconds, team_id, player_id, timestamp, description, coord_x, coord_y, team_with_possession_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (game_id, row['period'], row['game_clock'], row['elapsed_time_sec'],team_id, player_id, timestamp, row['event_description'], coord_x, coord_y, possession_team_id))
        play_by_play.commit()

        event_id = mycursor.lastrowid
        event_type = row['event_type']
        type = row['type']
        if event_type == 'rebound':
            mycursor.execute("SELECT id FROM rebounds WHERE type=%s", (row['rebound_type'],))
            rebound_id = None if rebound_id == 0 else mycursor.fetchone()[0]
        else: rebound_id = None
        if event_type == 'assist':
            mycursor.execute("SELECT id FROM assists WHERE type=%s", (row['type'],))
            assist_id = None if mycursor.rowcount == 0 else mycursor.fetchone()[0]
        else: assist_id = None
        if event_type == 'turnover':
            mycursor.execute("SELECT id FROM turnovers WHERE type=%s", (row['turnover_type'],))
            turnover_id = None if mycursor.rowcount == 0 else mycursor.fetchone()[0]
        else: turnover_id = None
        if type == 'fieldgoal' or type == 'freethrow':
            # Clean data
            shot_made = True if row['shot_made'] == 'true' else (False if row['shot_made'] == 'false' else None)
            points = int(row['points_scored']) if row['points_scored'] else None
            mycursor.execute("SELECT id FROM shots WHERE type=%s AND sub_type=%s AND basket_type=%s AND points=%s AND made=%s", (row['type'], row['shot_type'], row['shot_subtype'], points, shot_made))
            shot_id = None if mycursor.rowcount == 0 else mycursor.fetchone()[0]
        else: shot_id = None
        if 'timeout' in str(event_type):
            mycursor.execute("SELECT id FROM timeouts WHERE type=%s", (event_type,))
            timeout_id = None if mycursor.rowcount == 0 else mycursor.fetchone()[0]
        else: timeout_id = None

        mycursor.execute("INSERT INTO event_types (event_id, type, rebound_id, shot_id, turnover_id, assist_id, timeout_id) VALUES (%s, %s, %s, %s, %s, %s, %s)", (event_id, event_type, rebound_id, shot_id, turnover_id, assist_id, timeout_id))
        play_by_play.commit()

# Populate event types table
def populate_event_types(rows):
    '''Takes in a list of lists; populates the event types table'''
    # Delete existing rows in table
    mycursor.execute("SET foreign_key_checks = 0")
    mycursor.execute("DELETE FROM event_types")
    play_by_play.commit()
    mycursor.execute("SET foreign_key_checks = 1")
    print(mycursor.rowcount, "event types deleted")

    event_types = set()
    for row in rows:
        # Add event type if not yet in table
        if (row['event_type']) not in event_types:
            # Insert row
            print(row['event_type'])
            mycursor.execute("INSERT INTO event_types (name) VALUES (%s)", (row['event_type'],))
            event_types.add(row['event_type'])
            play_by_play.commit()
            print("Event type added")
        else:
            # print("No event types added")
            pass
    return rows


# Add home basket to games table

# populate_rebounds(rows)
# populate_shots(rows)
# populate_timeouts(rows)
# populate_turnovers(rows)
# populate_divisions(rows)
# populate_conferences(rows)
# populate_teams(rows)
# populate_players(rows)
# populate_tournaments(rows)
# populate_venues(rows)
# populate_games(rows)
# populate_events(rows)
create_entries(rows)