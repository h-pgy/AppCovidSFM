from db_tools.replicador_banco import ReplicadorMySql
from config import CONN_DIC_HAGAPE, TABLE_MDATA, PATH_BANCO_HP

class ExtratorHagape:

    def __init__(self, replicador = None, path_banco = None):

        self.path_banco = path_banco or + PATH_BANCO_HP
        self.replicador = replicador or ReplicadorMySql(CONN_DIC_HAGAPE, self.path_banco)

    def extrair_banco(self, table_mdata):

        self.replicador(table_mdata)

    def __call__(self, table_mdata = None):

        if table_mdata is None:
            table_mdata = TABLE_MDATA
        self.extrair_banco(table_mdata)
