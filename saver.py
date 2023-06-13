# -*- coding: utf-8 -*
# 负责将数据存入数据库

import pymysql.cursors
import redis


def get_connection():
    conn = pymysql.connect(host='localhost',
                            user='root',
                            password='5533',
                            db='lol_crawler',
                            charset='utf8mb4',
                            cursorclass=pymysql.cursors.DictCursor)
    return conn


def get_redis():
    client = redis.StrictRedis(host='localhost', port=6379, db=0)
    return client


# 初始化"比赛id"缓存
def build_cache_match_id():
    # 清楚旧缓存
    key = "match_id_set"
    redis_client = get_redis()
    redis_client.delete(key)

    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            sql = "SELECT `match_id` FROM `battle_info`"
            cursor.execute(sql)
            result = cursor.fetchall()
            for row in result:
                match_id = row['match_id']
                redis_client.sadd(key, match_id)
    finally:
        conn.close


# 初始化"比赛id-召唤师id"缓存
def build_cache_match_summoner_id():
    # 清楚旧缓存
    key = "match_summoner_id_set"
    redis_client = get_redis()
    redis_client.delete(key)

    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            sql = "SELECT `match_id`, `summoner_id` FROM `player_in_match`"
            cursor.execute(sql)
            result = cursor.fetchall()
            for row in result:
                value = row['match_id'] + '-' + row['summoner_id']
                redis_client.sadd(key, value)
    finally:
        conn.close


# 判断比赛是否已存在
def match_exist(match_id):
    redis_client = get_redis()
    return redis_client.sismember('match_id_set', match_id)


# 判断"玩家-比赛"数据是否存在
def match_player_exist(match_id, sid):
    redis_client = get_redis()
    return redis_client.sismember('match_summoner_id_set', match_id + '-' + sid)


# 保存玩家信息
def save_player(sid, name):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = "INSERT INTO `player` (`summoner_id`, `name`) VALUES (%s, %s)"
            cursor.execute(sql, (sid, name))
        connection.commit()
    finally:
        connection.close()


# 保存比赛详细信息
def save_match_detail(match_id, match_mode, match_type, match_creation, match_duration, map_id, match_version, participant_identities, participants, teams):
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            sql = "UPDATE `battle_info` SET `match_mode`=%s, `match_type`=%s, `match_creation`=%s, `match_duration`=%s, `map_id`=%s, `match_version`=%s, `participant_identities`=%s, `participants`=%s, `teams`=%s where `match_id`=%s"
            cursor.execute(sql, (match_mode, match_type, match_creation, match_duration, map_id, match_version, participant_identities, participants, teams, match_id))
        conn.commit()
    finally:
        conn.close


# 保存"玩家-比赛"战斗数据
def save_player_battle_detail(match_creation, match_duration, team_id, winner, kill, death, assists, kda, battle_involve_rate, cs, gold, gold_proportion, damage, damage_proportion, wards_placed, wards_killed, match_id, champion_id):
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            # Create a new record
            sql = "UPDATE `player_in_match` SET `match_creation`=%s, `match_duration`=%s, `team_id`=%s, `winner`=%s, `kill`=%s, `death`=%s, `assists`=%s, `kda`=%s, `battle_involve_rate`=%s, `cs`=%s, `gold`=%s, `gold_proportion`=%s, `damage`=%s, `damage_proportion`=%s, `wards_placed`=%s, `wards_killed`=%s where `match_id`=%s and `champion_id`=%s"
            cursor.execute(sql, (match_creation, match_duration, team_id, winner, kill, death, assists, kda, battle_involve_rate, cs, gold, gold_proportion, damage, damage_proportion, wards_placed, wards_killed, match_id, champion_id))
        conn.commit()
    finally:
        conn.close




