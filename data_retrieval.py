import ubctgdb as db
import pandas as pd
import matplotlib.pyplot as plt

# Example 1: grab everything
sql_all = '''
SELECT *
FROM Consumer_Sentiment;
'''
cs_all = db.run_sql(sql_all)
print(cs_all.head())

# Example 2: date-bounded query
start, end = '2020-01-01', '2021-12-31'
sql_window = f'''
SELECT date, umcsent
FROM Consumer_Sentiment
WHERE date BETWEEN '{start}' AND '{end}'
ORDER BY date;
'''
cs_window = db.run_sql(sql_window)
print(cs_window)

# Example 3: force a fresh pull (bypass cache)
cs_fresh = db.run_sql(sql_window, refresh=True)

cs_fresh['umcsent'] = pd.to_numeric(cs_fresh['umcsent'], errors='coerce')

print(cs_fresh.dropna().describe())
#plt.hist(cs_fresh['umcsent'])
#plt.show()

plt.plot(cs_fresh['date'], cs_fresh['umcsent'], marker='o', linestyle='-', color='green', label='y = x^2')
plt.show()
