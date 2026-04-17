from sc_reader import MySQLConfig, SCReader

config = MySQLConfig.from_json()
reader = SCReader(config=config)
print("Latest 5 rows from piddata:")
df = reader.preview_table_data("piddata")
print(df)
print("\nLatest 5 rows from runlidata:")
df2 = reader.preview_table_data("runlidata")
print(df2)
reader.close()
