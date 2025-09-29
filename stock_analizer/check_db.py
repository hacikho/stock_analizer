import sqlite3

conn = sqlite3.connect('data.db')
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print("Tables:", tables)

for table in tables:
    print(f"\nData from {table[0]}:")
    cursor.execute(f"SELECT * FROM {table[0]} LIMIT 5;")
    rows = cursor.fetchall()
    for row in rows:
        print(row)

conn.close()