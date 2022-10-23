import requests
import time


class FaceitApi():
    def __init__(self,token):
        self.__api_token = token
        self.__base_url = "https://open.faceit.com/data/v4"
        self.__headers = {
            'accept': 'application/json',
            'Authorization': F'Bearer {self.__api_token}'
        }
        
    def get_players_id_and_meta_stats(self,match_id):
        """Получение всех id игроков в матче"""
        try:
            players_id = []
            meta_stats = []
            api_url = f"{self.__base_url}/matches/{match_id}"
            response = requests.get(api_url, headers=self.__headers)
            res = response.json()
            for team1 in res['teams']['faction1']['roster']:
                players_id.append(team1['player_id'])
                
                meta_stats.append(tuple([res['match_id'],team1['player_id'],res['started_at'],team1['membership'],
                                  team1['anticheat_required'],team1['game_skill_level'],
                                  res['teams']['faction1']['faction_id'],'faction1',res['results']['winner']]))
                
            for team2 in res['teams']['faction2']['roster']:
                players_id.append(team2['player_id'])
                meta_stats.append(tuple([res['match_id'],team2['player_id'],res['started_at'],team2['membership'],
                                  team2['anticheat_required'],team2['game_skill_level'],
                                  res['teams']['faction2']['faction_id'],'faction2',res['results']['winner']]))
                
            return [players_id,meta_stats,res['started_at']]
        
        except Exception as e:
            
            if response.status_code == 429:
                print('Слишком много запросов')
                time.sleep(30)
            else:
                print(e,'get_players_id_and_meta_stats')
            return response.status_code
    
    
    def get_last_matches_id(self,player_id,to,game='csgo',limit=20):
        """Получение id игр у игрока"""
        try:
            id_matches = []
            api_url = F"{self.__base_url}/players/{player_id}/history?game={game}&limit={limit}&to={to}"
            response = requests.get(api_url, headers=self.__headers)
            res = response.json()
            for match in res['items']:
                if match['game_mode']=='5v5':
                    id_matches.append(match['match_id'])
            return id_matches
        except Exception as e:
            if response.status_code == 429:
                print('Слишком много запросов')
                time.sleep(30)
            else:
                print(e,'get_last_matches_id')
            return response.status_code
        
        
    def get_last_5_match(self,player_id,game='csgo',limit=2):
        
        try:
            id_matches = []
            api_url = F"{self.__base_url}/players/{player_id}/history?game={game}&limit={limit}"
            response = requests.get(api_url, headers=self.__headers)
            res = response.json()
            for match in res['items']:
                if match['game_mode']=='5v5':
                    id_matches.append(match['match_id'])
            return id_matches
        except Exception as e:
            if response.status_code == 429:
                print('Слишком много запросов')
                time.sleep(30)
            else:
                print(e,'get_last_5_match')
            return response.status_code
    
    
    def get_stats_of_match(self,match_id,player_id):
        try:
            players_stats = []
            api_url_stats = F"{self.__base_url}/matches/{match_id}/stats"
            
            response = requests.get(api_url_stats, headers=self.__headers)
            res_stats = response.json()['rounds'][0]
            for team in res_stats['teams']:
                for player in team['players']:
                    if player['player_id']==player_id:
                        players_stats = tuple([res_stats['match_id'],player['player_id'],res_stats['round_stats']['Score'],
                             team['team_stats']['Overtime score'],team['team_stats']['First Half Score'],
                             team['team_stats']['Second Half Score'],team['team_stats']['Final Score'],res_stats['round_stats']['Map'],
                            player['player_stats']['Deaths'],player['player_stats']['Headshots %'],
                             player['player_stats']['Headshots'],player['player_stats']['Kills'],
                            player['player_stats']['K/D Ratio'],player['player_stats']['Penta Kills'],
                             player['player_stats']['Quadro Kills'],player['player_stats']['Triple Kills'],
                             player['player_stats']['MVPs'],player['player_stats']['K/R Ratio'],player['player_stats']['Assists'],
                             player['player_stats']['Result']
                            ])
                        break
            return players_stats
        except Exception as e:
            if response.status_code == 429:
                print('Слишком много запросов')
                time.sleep(30)
            else:
                print(e,'get_stats_of_match')
            return response.status_code
    
    def get_all_statistics(self,player_id):
        
        try:
            map_stats = {}
            api_url = F"{self.__base_url}/players/{player_id}"
            res1 = requests.get(api_url,headers=self.__headers)
            level = res1.json()['games']['csgo']['skill_level']
            elo = res1.json()['games']['csgo']['faceit_elo']

            api_url = F"{self.__base_url}/players/{player_id}/stats/csgo"
            res2 = requests.get(api_url, headers=self.__headers)
            res2_life,res2_segm = res2.json()['lifetime'], res2.json()['segments']
            del res2_life['K/D Ratio']
            del res2_life['Recent Results']
            timestamp = time.time()
            res2_life['player_id'] = player_id
            res2_life['elo'] = elo
            res2_life['level'] = level
            res2_life['time_parse'] = timestamp
            for map_cs in res2_segm:
                if map_cs['type']=='Map' and map_cs['mode']=='5v5':
                    map_stats[map_cs['label']] = map_cs['stats']
                    map_stats[map_cs['label']]['player_id'] = player_id
                    map_stats[map_cs['label']]['time_parse'] = timestamp
            
            return [res2_life,map_stats]
        except Exception as e:
            if res2.status_code == 429:
                print('Слишком много запросов')
                time.sleep(30)
            else:
                print(e,'get_all_statistics')
            return res2.status_code