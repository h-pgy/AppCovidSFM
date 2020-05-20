import sqlite3
import pandas as pd


def handle_cursor(func):
    '''Decorator para abrir e fechar cursor'''
    def wrapper(*args, **kwargs):
        self = args[0]
        cursor = self.con.cursor()
        kwargs['cursor'] = cursor
        returned = func(*args, **kwargs)
        cursor.close()

        return returned

    return wrapper


class ReadSqlite:
    '''Classe simples para leitura de banco sqlite3.'''

    def __init__(self, db_path):
        try:
            self.con = sqlite3.connect(db_path)
        except sqlite3.OperationalError as e:
            print(f'Provavel erro no path do banco: {db_path}')
            raise sqlite3.OperationalError(e)

    @handle_cursor
    def listar_tabelas(self, *ignore, cursor):

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
        return [tup[0] for tup in cursor.fetchall()]

    @handle_cursor
    def pegar_col_names(self, tb_name, *ignore, cursor):

        cursor.execute(f'PRAGMA table_info({tb_name});')
        col_names = [tup[1] for tup in cursor.fetchall()]

        return col_names

    def solve_select_query(self, tb_name, **kwargs):

        query = f"SELECT * FROM {tb_name}"
        if kwargs.get('where'):
            query = query + ' ' + f"WHERE {kwargs['where']}"
        elif kwargs.get('limit'):
            query = query + ' ' + f"LIMIT {kwargs['limit']}"

        return query

    @handle_cursor
    def get_data(self, tb_name, where_clause=None, *ignore, cursor, **kwargs):

        query = self.solve_select_query(tb_name, **kwargs)
        cursor.execute(query)

        return cursor.fetchall()

    def table_as_df(self, tb_name, limit=None, where=None):
        '''Retorna resultado de query como dataframe'''

        cols = self.pegar_col_names(tb_name)
        data = self.get_data(tb_name, where, limit=limit, where=where)

        return pd.DataFrame(data=data, columns=cols)

