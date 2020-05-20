from db_tools.read_sqlite import ReadSqlite
from config import PATH_BANCO_APP
from core.proj_exceptions import EmAtualizacao

class DAO:

    def __init__(self, app_status, path_banco_app = None):

        self.path_banco_app = path_banco_app or PATH_BANCO_APP
        self.reader = ReadSqlite(self.path_banco_app)
        self.status = app_status

    def retornar_df(self, tb_name):

        if self.status.banco_em_atualizacao:
            raise EmAtualizacao

        return self.reader.table_as_df(tb_name)