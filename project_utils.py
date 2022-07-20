import cx_Oracle
import requests
import pandas as pd
from tqdm import tqdm
import random
from random import sample
import matplotlib.pyplot as plt
import plotly
import plotly.graph_objs as go
plotly.offline.init_notebook_mode(connected=True)
import plotly.express as px
from matplotlib import font_manager, rc
font_path="C:/\Windows/Fonts/gulim.ttc"
font=font_manager.FontProperties(fname=font_path).get_name()
rc('font',family=font) #윈도우


dsn = cx_Oracle.makedsn('localhost', 1521, 'xe')
lol_api_key='RGAPI-e3a5191d-ebc5-401d-8de1-b87591af93dd'


def db_open():
    global db
    global cursor
    db = cx_Oracle.connect(user='GAMEI', password='1111', dsn=dsn)
    cursor = db.cursor()
    print('open!')

def sql_execute(q):
    global db
    global cursor

    try:
        if 'selec' in q:
            df=pd.read_sql(sql=q, con=db)
            return df
        cursor.execute(q)
        return 'success!'
    except Exception as e:
        print(e)

def db_close():
    global db
    global cursor

    try:
        db.commit()
        cursor.close()
        db.close()
        return 'close!'
    except Exception as e:
        print(e)
        
def df_creater(url):
    url = url.replace('(인증키)',api_key).replace('xml','json').replace('/5/','/1000/')
    res = requests.get(url).json()
    key = list(res.keys())[0]
    data = res[key]['row']
    df = pd.DataFrame(data)
    return df
        
def get_puuid(user):
    url='https://kr.api.riotgames.com/lol/summoner/v4/summoners/by-name/'+user+'?api_key='+lol_api_key
    res=requests.get(url).json()
    puuid=res['puuid']
    return puuid

def get_matchid(puuid, num):
    url='https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/'+puuid+'/ids?start=0&count='+str(num)+'&api_key='+lol_api_key
    res=requests.get(url).json()
    return res


def get_matches_timelines(matchids):
    lst = []
    for matchid in tqdm(matchids):
        tmp =[]
        url = 'https://asia.api.riotgames.com/lol/match/v5/matches/'+matchid+'?api_key='+lol_api_key
        res1 = requests.get(url).json()
        url = 'https://asia.api.riotgames.com/lol/match/v5/matches/'+matchid+'/timeline?api_key='+lol_api_key
        res2 = requests.get(url).json()
        tmp.append([matchid,res1,res2])
        lst.extend(tmp)
    return lst

def get_rawData(tier):
    division_list=['I', 'II', 'III', 'IV']
    p=random.randrange(1,20)
    lst=[]
    for division in division_list:
        url='https://kr.api.riotgames.com/lol/league/v4/entries/RANKED_SOLO_5x5/'+tier+'/'+division+'?page='+str(p)+'&api_key='+lol_api_key
        res=requests.get(url).json()
        lst+=sample(res,5)
    
    print('get SummonerName.....')
    summonerName_lst=list(map(lambda x:x['summonerName'],lst))
    
    print('get puuid......')
    puuid_lst=[]
    for n in tqdm(summonerName_lst):
        try:
            puuid_lst.append(get_puuid(n))
        except:
            print(n)
            continue
            
    print('get match_id.....')
    matchid_lst=[]
    for p in tqdm(puuid_lst):
        matchid_lst.extend(get_matchid(p,2))
        
    print('get matches & timeline....')
    match_timeline_lst=get_matches_timelines(matchid_lst)
    
    df=pd.DataFrame(match_timeline_lst, columns=['gameId','matches','timeline'])
    print('complete!')   
    return df

def get_match_df(df):
    df_creater=[]
    print('match_df 생성중...')
    for i in tqdm(range(len(df))):
        try:
            if df.iloc[i].matches['info']['gameMode']=='CLASSIC':
                for j in range(10):
                    tmp=[]
                    tmp.append(df.iloc[i].gameId)
                    tmp.append(df.iloc[i].matches['info']['gameDuration'])
                    tmp.append(df.iloc[i].matches['info']['gameVersion'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['summonerName'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['summonerLevel'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['participantId'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['championName'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['champExperience'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['teamPosition'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['teamId'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['win'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['kills'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['deaths'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['assists'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['totalDamageDealtToChampions'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['totalDamageTaken'])

                    df_creater.append(tmp)
        except:
            continue
    
    columns=['gameId', 'gameDuration', 'gameVersion', 'summonerName', 'summonerLevel', 'participantId', 'championName', 'champExperience',
             'teamPosition', 'teamId', 'win', 'kills', 'deaths', 'assists', 'totalDamageDealtToChampions', 'totalDamageTaken']
    
    df=pd.DataFrame(df_creater, columns=columns).drop_duplicates()
    print('complete! 현재 df의 수는 %d입니다'%len(df))
    return df
             
def get_timeline_df(df):
    df_creater=[]
    print('timeline_df 생성중...')
    for i in tqdm(range(len(df))):
        try:
            if df.iloc[i].matches['info']['gameMode']=='CLASSIC':
                for j in range(10):
                    tmp=[]
                    tmp.append(df.iloc[i].gameId)
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['participantId'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['teamPosition'])

                    for k in range(5,21):
                        try:
                            tmp.append(df.iloc[i].timeline['info']['frames'][k]['participantFrames'][str(j+1)]['totalGold'])
                        except:
                            tmp.append(0)

                    df_creater.append(tmp)
        except:
            continue
    
    columns=['gameId','participantId','teamPosition','g5','g6','g7','g8','g9','g10','g11','g12','g13','g14','g15','g16','g17','g18','g19','g20']
    
    df=pd.DataFrame(df_creater, columns=columns).drop_duplicates()
    print('complete! 현재 df의 수는 %d입니다'%len(df))
    return df
def insert_matches(row):
    query=(f'MERGE INTO match USING DUAL ON(gameId=\'{row.gameId}\'and participantId={row.participantId}) '
           f'WHEN NOT MATCHED THEN '
           f'insert (gameId, gameDuration, gameVersion, summonerName, summonerLevel, participantId,'
           f'championName, champExperience, teamPosition, teamId, win,'
           f'kills, deaths, assists, totalDamageDealtToChampions, totalDamageTaken)'
           f'values'
           f'(\'{row.gameId}\',{row.gameDuration},\'{row.gameVersion}\',\'{row.summonerName}\',{row.summonerLevel},'
           f'{row.participantId},\'{row.championName}\',{row.champExperience},\'{row.teamPosition}\',{row.teamId},\'{row.win}\','
           f'{row.kills},{row.deaths},{row.assists},{row.totalDamageDealtToChampions},{row.totalDamageTaken})'
          )
    sql_execute(query)
    return

def insert_timeline(row):
    query=(f'MERGE INTO timeline USING DUAL ON(gameId=\'{row.gameId}\'and participantId={row.participantId}) '
           f'WHEN NOT MATCHED THEN '
           f'insert (gameId, participantId,teamPosition,'
           f'g5,g6,g7,g8,g9,g10,g11,g12,g13,g14,g15,g16,g17,g18,g19,g20)'
           f'values'
           f'(\'{row.gameId}\',{row.participantId},\'{row.teamPosition}\',{row.g5},{row.g6},{row.g7},{row.g8},{row.g9},{row.g10},'
           f'{row.g11},{row.g12},{row.g13},{row.g14},{row.g15},{row.g16},{row.g17},{row.g18},{row.g19},{row.g20})'
          )
    sql_execute(query)
    return