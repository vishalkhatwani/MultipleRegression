# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.


import psycopg2

conn = psycopg2.connect(host="localhost", database="first_bigdata_app", user="postgres", password="postgres")
cur = conn.cursor()

#df = cur.execute("SELECT * FROM public.cars LIMIT 100;")

#print(df)
