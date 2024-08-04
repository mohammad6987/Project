from google_play_scraper import search,app,reviews,Sort
import psycopg2
from psycopg2 import sql
import redis
import time
DB_PARAMS = {
    'dbname': 'dbtest',
    'user': 'postgres',
    'password': 'root',
    'host': 'localhost',
    'port': '5432'        
}

REDIS_PARAMS = {
    'host': 'localhost',
    'port': 6379,
    'db': 0
}


def is_app_available(app_name):
    try:
        results = search(
            app_name,
            lang='en',
            country='us',
            n_hits=5,
        )
        for app in results:
            if app_name.lower() in app['title'].lower():
                print(f"App '{app_name}' is available on Google Play Store.")
                print(f"App ID: {app['appId']}")
                print(f"Developer: {app['developer']}")
                print(f"Score: {app['score']}")
                return app['appId']
        
        print(f"App '{app_name}' is not available on Google Play Store.")
        return None
    except Exception as e:
        print(f"Error occurred: {e}")
        return None

def get_app_info(appID):
    app_info = app(appID, lang='en', country='us')
    print('got app info')
    return app_info


def get_app_reviews(appID):
    app_reviews = reviews(appID , lang='en', country='us' , sort = Sort.NEWEST , count= 1000 )
    print('got app reviews')
    return app_reviews



def save_app_info_in_database(app_info , conn):
    cursor = conn.cursor()
    query = '''
    INSERT INTO appsInfo (
        appId, title, minInstalls , score , ratings , reviewsCount , updated , version , adSupported
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (appId) DO UPDATE SET
        title = EXCLUDED.title,
        minInstalls = EXCLUDED.minInstalls,
        score = EXCLUDED.score,
        ratings = EXCLUDED.ratings,
        reviewsCount = EXCLUDED.reviewsCount,
        updated = EXCLUDED.updated,
        version = EXCLUDED.version,
        adSupported = EXCLUDED.adSupported
    '''
    cursor.execute(query, [app_info['appId'], app_info['title'], app_info['minInstalls'],
                           app_info['score'],app_info['ratings'],app_info['reviews'], 
                           app_info['updated'], app_info['version'],app_info['adSupported']])
    conn.commit()

def save_app_reviews_in_database(appReviews , appID , redis_conn):
    try:
        redis_key = f"app_reviews:{appID}"
        redis_conn.delete(redis_key)
        
        print(f"size of reviews : {len(appReviews[0])}")
        for review in appReviews[0]:
            review_data = {
                'reviewId': review.get('reviewId'),
                'userName': review.get('userName'),
                'score': review.get('score'),
                'text': review.get('content'),
                'date': review.get('at').isoformat(),
                'thumbsUp' : review.get('thumbsUpCount')
            }

            #redis_conn.hset(f"{redis_key}:{review.get('reviewId')}", review_data)
            redis_conn.hset(f"{redis_key}:{review.get('reviewId')}", mapping=review_data)
        
        print(f"Reviews for app ID {appID} saved to Redis.")
    except Exception as e:
        print(f"Error saving reviews to Redis: {e}")


def connect_to_db():
    """Establish a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        print("Connection successful.")
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None
    
def connect_to_redis():
    """Establish a connection to the Redis database."""
    try:
        redis_conn = redis.StrictRedis(**REDIS_PARAMS)
        print("Connection to Redis successful.")
        return redis_conn
    except Exception as e:
        print(f"Error connecting to Redis: {e}")
        return None



def create_table(conn, tableName):
    """Create a table in the PostgreSQL database with a unique constraint."""
    try:
        with conn.cursor() as cursor:
            create_table_query = sql.SQL('''
                CREATE TABLE IF NOT EXISTS {table} (
                    id SERIAL PRIMARY KEY,
                    app_name VARCHAR(100),
                    app_id VARCHAR(100) UNIQUE
                );
            ''').format(
                table=sql.Identifier(tableName)
            )
            cursor.execute(create_table_query)
            conn.commit()
            print("Table created successfully.")
    except Exception as e:
        print(f"Error creating table: {e}")

def create_appsInfo_table(conn):
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS appsInfo (
        appId TEXT PRIMARY KEY,
        title TEXT,
        minInstalls BIGINT,
        score REAL,
        ratings INTEGER,
        reviewsCount INTEGER,
        updated INTEGER,
        version TEXT,
        adSupported BOOLEAN
    )
    ''')
    print('database for appsInfo is created')
    conn.commit()


def insert_apps_in_database(conn, tableName, appsList):
    """Insert app names and their IDs into the specified table."""
    try:
        with conn.cursor() as cursor:
            for app_name in appsList:
                app_id = is_app_available(app_name)
                if app_id:
                    insert_query = sql.SQL('''
                        INSERT INTO {table} (app_name, app_id)
                        VALUES (%s, %s)
                        ON CONFLICT (app_id) DO NOTHING;
                    ''').format(
                        table=sql.Identifier(tableName)
                    )
                    cursor.execute(insert_query, (app_name, app_id))
            conn.commit()
            print("Apps inserted successfully.")
    except Exception as e:
        print(f"Error inserting apps into database: {e}")


def get_apps_from_database(conn, tableName):
    """Retrieve a list of apps from the database and return them as a list."""
    try:
        with conn.cursor() as cursor:
            query = sql.SQL('SELECT app_name, app_id FROM {table};').format(
                table=sql.Identifier(tableName)
            )
            cursor.execute(query)
            results = cursor.fetchall()  # Fetch all rows
            # Convert the results to a list of tuples (app_name, app_id)
            apps_list = [{'app_name': row[0], 'app_id': row[1]} for row in results]
            return apps_list
    except Exception as e:
        print(f"Error retrieving apps from database: {e}")
        return []




def main():
    """Main function to demonstrate PostgreSQL operations."""
    conn = connect_to_db()
    redis_conn = connect_to_redis()
    redis_conn.flushall()
    if conn == None:
        return 0
    create_appsInfo_table(conn= conn)
    apps_list = get_apps_from_database(conn , 'appsList')
    for littleApp in apps_list:
        app_info  =get_app_info(littleApp['app_id'])
        app_reviews = get_app_reviews(littleApp['app_id'])
        save_app_info_in_database(app_info , conn)
        save_app_reviews_in_database(app_reviews , littleApp['app_id'] , redis_conn)
    conn.close()
    redis_conn.close()
if __name__ == '__main__':
    main()
