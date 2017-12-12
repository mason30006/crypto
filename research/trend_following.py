import utils.sql as sql

conn = sql.connect_to_db('BTC_DB')
query = 'SELECT TOP 1000 *' \
        'FROM dbo.hist_minute_bars'
data = sql.execute(query, conn)
