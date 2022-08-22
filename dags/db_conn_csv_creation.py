from sqlalchemy import create_engine, text
import csv
from logging_config import logger


def create_csv_from_sql(database_url, sql_query_path, output_csv_path):
    """
    This function connects to a db, excecutes an sql query and saves the result in a csv in a destination path

    Parameters
    ----------
    database_url : str
        database connection url
    sql_query_path : str
        sql file path
    output_csv_path : str
        csv file path
    """

    engine = create_engine(database_url)

    with engine.connect() as con:
        with open(sql_query_path) as file:
            query = text(file.read())
            cursor = con.execute(query)

    with open(output_csv_path, "w") as outfile:
        outcsv = csv.writer(outfile)
        # dump column titles
        outcsv.writerow(x for x in cursor.keys())
        # dump rows
        outcsv.writerows(cursor.fetchall())
