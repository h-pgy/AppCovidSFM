from core.extrator_banco import ExtratorHagape
from core.merge_tables import TableMerger
from config import PATH_BANCO_HP, PATH_BANCO_APP
import sqlite3

class Updater:

    def __init__(self, path_banco_hp = None, path_banco_app = None):

        self.path_banco_app = path_banco_app or PATH_BANCO_APP
        self.path_banco_hp = path_banco_hp or PATH_BANCO_HP
        db_app = sqlite3.connect(self.path_banco_app)
        self.conn = db_app
        self.extrator = ExtratorHagape(path_banco=self.path_banco_hp)
        self.merger = TableMerger(path_banco=self.path_banco_hp,
                                  conn_banco_app = self.conn)

    def atualizar_hp(self):

        self.extrator()

    def atualizar_app(self):

        dados_obito, cemiterio, \
        contratacao, cremacao, sepult = self.merger.gerar_todas_tables(save = True)

        return dados_obito, cemiterio, \
        contratacao, cremacao, sepult

    def update_all(self):

        self.atualizar_hp()
        print('Banco replicado atualizado')
        print('Atualizando banco app')
        self.atualizar_app()
        print('Banco app atualizado')


