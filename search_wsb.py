from psaw import PushshiftAPI
import datetime

import config
import alpaca_trade_api as tradeapi
import psycopg2
import psycopg2.extras
import csv
import locale

locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
connection = psycopg2.connect(host=config.DB_HOST, database=config.DB_NAME, user=config.DB_USER, password=config.DB_PASS)

cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

cursor.execute('SELECT * FROM stock')
rows = cursor.fetchall()
stocks = {}

for row in rows:
  stocks['$' + row['symbol']] = row['id']

# print(stocks)

api = PushshiftAPI()

start_time = int(datetime.datetime(2021, 10, 31).timestamp())

submissions= list(api.search_submissions(after=start_time,
          subreddit='wallstreetbets', 
          filter=['url', 'author', 'title', 'subreddit']))

for submission in submissions:
  # print(submission.created_utc)
  # print(submission.title)
  # print(submission.url)

  words = submission.title.split()
  cashtags = list(set(filter(lambda word: word.lower().startswith('$'), words)))
  
  if len(cashtags) > 0:
    print(cashtags)
    print(submission.title)

    for cashtag in cashtags:
      post_time = datetime.datetime.fromtimestamp(submission.created_utc).isoformat()
      if not cashtag[-1].isalpha(): 
        cashtag = cashtag.replace(cashtag[-1], '')
      
      if cashtag not in stocks:
        continue

      try:
        cursor.execute("""
          INSERT INTO mention (dt, stock_id, message, source, url)
          VALUES (%s, %s, %s, 'wallstreetbets', %s)
        """, (post_time, stocks[cashtag], submission.title, submission.url))

        connection.commit()
      except Exception as e:
        print(e)
        connection.rollback()