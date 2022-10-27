import requests
import time
from psycopg2 import OperationalError
import psycopg2
from config import settings
from modules import FaceitApi
import asyncio
import httpx


token = settings.API_KEY
db_name = settings.DBNAME
db_user = settings.USERDB
db_password = settings.PASSWORDDB
db_host = settings.ADRESSDB
db_port = settings.PORTDB

map_stats_columns = ['map_cs','player_id','KD','Average_Quadro_Kills','Average_MVPs','Average_Assists',
					 'Penta_Kills','Headshots','Average_Kills','Quadro_Kills','Wins','Win_Rate',
					 'Matches','Triple_Kills','Headshots_per_Match','Average_KR','Average_Deaths',
					 'Total_Headshots','KR','Average_Triple_Kills','Average_Headshots','Rounds',
					 'Assists','MVPs','Kills','Deaths','Average_Penta_Kills','Average_KD','time_parse']
players_previous_stats_columns = ['match_id','player_id','Score','Overtime_score',
								 'First_Half_Score','Second_Half_Score','Final_Score',
								  'map_csgo','Deaths','Headshots_per','Headshots','Kills','KD',
								  'Penta_Kills','Quadro_Kills','Triple_Kills','MVPs','KR','Assists','result_match']
columnsdb_life_stats = {
	'Current Win Streak':'Current_Win_Streak',
	'Average Headshots %': 'Average_Headshots',
	'Matches':'Matches',
	'Average K/D Ratio':'Average_KD',
	'Win Rate %':'Win_Rate',
	'Wins':'Wins',
	'Longest Win Streak':'Longest_Win_Streak',
	'Total Headshots %':'Total_Headshots',
	'player_id':'player_id',
	'elo':'elo',
	'level':'faceit_level',
	'time_parse':'time_parse'
}
query = """
		select 
			distinct mim.match_id
		from 
			raw_data.meta_info_match mim
		"""

def create_connection(db_name, db_user, db_password, db_host, db_port):
	connection = None
	try:
		connection = psycopg2.connect(
			database=db_name,
			user=db_user,
			password=db_password,
			host=db_host,
			port=db_port,
		)
		print("Connection to PostgreSQL DB successful")
	except OperationalError as e:
		print(f"The error '{e}' occurred")
	return connection


def execute_read_query(connection, query):
	cursor = connection.cursor()
	result = None
	try:
		cursor.execute(query)
		result = cursor.fetchall()
		return result
	except OperationalError as e:
		print(f"The error '{e}' occurred")

def insert_values(connection, schema='railway', table='', values=list(), columns=list()):
	try:
		records = ", ".join(["%s"] * len(values))

		insert_query = (
			f"INSERT INTO {schema}.{table} ({', '.join(columns)}) VALUES {records}"
		)

		connection.autocommit = True
		cursor = connection.cursor()
		cursor.execute(insert_query, values)
		print(f'Записали данные в {table}')
	except Exception as e:
		print(f'{time.ctime()} {e} insert_values')
		f = open('errors.txt', 'a+')
		f.write(f'{time.ctime()} {e} insert_values'+'\n')
		f.close()

async def get_stats(last_matches,player):
	async with httpx.AsyncClient() as client:
		stats = []
		for prev_match in last_matches:
			stats.append(asyncio.create_task(fa.get_stats_of_match(client,prev_match,player)))
										
		stats_previos_match = await asyncio.gather(*stats)
		values = list(filter((704).__ne__, stats_previos_match))
		return values

async def get_stats_speed(last_matches,player):
	async with httpx.AsyncClient() as client:
		stats = []
		for i,prev_match in enumerate(last_matches):
			stats.append(asyncio.create_task(fa.get_stats_of_match(client,last_matches[i],player[i])))
										
		stats_previos_match = await asyncio.gather(*stats)
		return stats_previos_match

async def get_info(players,timestamp_to,was_parsing,next_matches):
	async with httpx.AsyncClient() as client:
		all_stats = []
		prev_match_20 = []
		last_5_match = []
		for player_id in players:
			all_stats.append(asyncio.create_task(fa.get_all_statistics(client,player_id)))
			prev_match_20.append(asyncio.create_task(fa.get_last_matches_id(client,player_id,timestamp_to)))
			last_5_match.append(asyncio.create_task(fa.get_last_5_match(client,player_id)))

		all_stats_gather = await asyncio.gather(*all_stats)
		prev_match_20_gather = await asyncio.gather(*prev_match_20)
		last_5_match_gather = await asyncio.gather(*last_5_match)

		for i,gather_match in enumerate(prev_match_20_gather):
			if last_5_match_gather[i]!=703:
				for m in last_5_match_gather[i]:
					if (m not in was_parsing) and (m not in next_matches):
							next_matches.append(m)
			map_stats = []
			if all_stats_gather[i]!=705:
				life_stats = all_stats_gather[i][0]
				life_stats_columns = []
				life_stats_values = [tuple(life_stats.values())]
				for key,value in life_stats.items():
					life_stats_columns.append(columnsdb_life_stats[key])

				insert_values(connection,schema='raw_data',table='life_stats',
				 			values=life_stats_values,columns=life_stats_columns)

				for key,value in all_stats_gather[i][1].items():
					map_stats.append(tuple([key]+
						[value['player_id'],value['K/D Ratio'],value['Average Quadro Kills'],value['Average MVPs'],
						value['Average Assists'],value['Penta Kills'],value['Headshots'],value['Average Kills'],
						value['Quadro Kills'],value['Wins'],value['Win Rate %'],value['Matches'],value['Triple Kills'],
						value['Headshots per Match'],value['Average K/R Ratio'],value['Average Deaths'],value['Total Headshots %'],
						value['K/R Ratio'],value['Average Triple Kills'],value['Average Headshots %'],value['Rounds'],
						value['Assists'],value['MVPs'],value['Kills'],value['Deaths'],value['Average Penta Kills'],value['Average K/D Ratio'],
						value['time_parse']]))

				insert_values(connection,schema='raw_data',table='map_stats',values=map_stats,columns=map_stats_columns)
	
	return [next_matches,prev_match_20_gather]		


connection = create_connection(db_name,db_user,db_password,db_host,db_port)
was_parsing = [x[0] for x in execute_read_query(connection,query)]
fa = FaceitApi.FaceitApi(token)
connection = create_connection(db_name,db_user,db_password,db_host,db_port)
next_matches = ['1-44688bae-b4cc-4410-97fe-a9a5c81bae8c']


for match_id in next_matches:
	was_parsing.append(match_id)
	players_id = fa.get_players_id_and_meta_stats(match_id)
	if players_id !=701:
		players_id,meta_stats,timestamp_to = players_id[0],players_id[1],players_id[2]

		insert_values(connection,schema='raw_data',table='meta_info_match',
				values=meta_stats,columns=['match_id','player_id','started_at','membership',
				'anticheat_required','game_skill_level','team_id','faction','winner_team'])
		
		# x = asyncio.run(get_info(players = players_id,timestamp_to=timestamp_to,was_parsing = was_parsing,next_matches = next_matches))
		# previos_matches = []
		# previos_players = []
		# for z in x[1]:
		# 	previos_players += [z[1]]*len(z[0])
		# 	previos_matches += z[0]
		# stats_previos_match = asyncio.run(get_stats_speed(last_matches = previos_matches, player = previos_players))

		prev_gather_matches = asyncio.run(get_info(players = players_id,timestamp_to=timestamp_to,was_parsing = was_parsing,next_matches = next_matches))
		for gather_match in prev_gather_matches[1]:
			if gather_match[0]!=702:
				stats_previos_match = asyncio.run(get_stats(last_matches = gather_match[0], player = gather_match[1]))
				insert_values(connection,schema='raw_data',table='players_previous_stats',
				 		values=stats_previos_match,columns=players_previous_stats_columns)

