
def import_query(path):
    """_summary__
    Imports a SQL query from a file.

    Args:
        path ( String): _path to the SQL file

    Returns:
        _type_: SQL query string
    """
    with open(path, "r") as file:
        query = file.read()
    return query

def check_tables(spark, catalog, database, table):
    """_summary__
    Checks if a table exists in the specified catalog and database.

    Args:
        spark (_type_): Spark session
        catalog (str): Catalog name
        database (str): Database name
        table (str): Table name

    Returns:
        _type_: Boolean value indicating if the table exists
    """
    query = f"SHOW TABLES IN {catalog}.{database} LIKE '{table}'"
    result = spark.sql(query).count()
    return result > 0