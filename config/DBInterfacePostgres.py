import psycopg2

import pandas as pd 

from sqlalchemy import create_engine
from sqlalchemy import text

class DBInterface:
    def connection_settings(self, sql_type: str, username: str, password: str, hostname: str, server_name: str):
        self.sql_type    = sql_type
        self.username    = username
        self.password    = password 
        self.hostname    = hostname 
        self.server_name = server_name
        self.engine      = create_engine(f"{sql_type}://{username}:{password}@{hostname}/{server_name}", echo = False)
    
    def upload_to_database(self, table_name: str, df: pd.core.frame.DataFrame, exist_option: str = "append"):
        df.to_sql(table_name, con = self.engine, if_exists = exist_option, index = False)
        
    def get_from_database(self, table_id: str, columns_list: list[str], filter_condition: str = None):
        columns_list   = "*" if (columns_list == ["*"]) else [f'"{column}"' for column in columns_list]
        query_str      = (lambda x: x if (filter_condition == None) else f"{x} WHERE {filter_condition}")(f"SELECT {','.join(columns_list)} FROM \"{table_id}\"")
        retrieved_data = self.engine.connect().execute(text(query_str)).fetchall()

        return retrieved_data
        
    def delete_from_database(self, table_id: str, column_name: str,  filter_value: str, filter_condition: str = "equals"):
        filter_dict = {
            "equals" : "=",
            "gt"     : ">",
            "lt"     : "<",
            "gte"    : ">=",
            "lte"    : "<=",
            "in"     : "in"
        }

        query_str = f'DELETE FROM "{table_id}" WHERE "{column_name}" {filter_dict[filter_condition]} {filter_value}'
        
        with self.engine.connect() as connection:
            connection.execute(text(query_str))
            connection.commit()
