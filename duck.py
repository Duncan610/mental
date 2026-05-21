# Run this in a Python shell to verify DuckDB is working
import duckdb

conn = duckdb.connect("data/duckdb/mental_health_pulse.duckdb")
print("Connected successfully")
tables = conn.execute("SHOW TABLES").fetchall()
print(tables)