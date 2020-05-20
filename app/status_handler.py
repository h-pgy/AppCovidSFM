import json
import datetime
from config import APP_TABLES

class StatusHandler:

    def __init__(self, app):

        self.status = app.config.status

    def init_status(self):

        self.status['Banco'] = {'EmAtualizacao' : False, 'UltimaAtualizacao' : self.data_hoje()}
        self.status['mdata'] = {'initialized_at' : self.data_hoje()}
        self.status['Files'] = {}
        for table in APP_TABLES:
            self.status['Files'][table] = {'Criada' : False, 'Atualizada' : False}

    def data_hoje(self):

        agora = datetime.datetime.now()
        return agora.strftime("%b, %d %Y %H:%M:%S")

    @property
    def ultima_atualizacao_banco(self):
        return self.status['Banco']['UltimaAtualizacao']

    def ultima_atualizacao_banco_setter(self, data=None):
        data = data or self.data_hoje()
        self.status['Banco']['UltimaAtualizacao'] = data

    def banco_em_atualizacao_setter(self, em_atualizacao):

        self.status['Banco']['EmAtualizacao'] = em_atualizacao

    @property
    def banco_em_atualizacao(self):
        return self.status['Banco']['EmAtualizacao']

    def file_atualizada(self, tb_name):
        files = self.status['Files']
        if tb_name not in files:
            files[tb_name] = {}
            files[tb_name]['Criada'] = False
            files[tb_name]['Atualizada'] = False

        return files[tb_name]

    def atualizar_file(self, tb_name):

        if 'Files' not in self.status:
            self.status['Files'] = {}
        files = self.status['Files']
        if tb_name not in files:
            files[tb_name] = {}

        files[tb_name]['Criada'] = True
        files[tb_name]['Atualizada'] = True
        files[tb_name]['Dt_criacao'] = self.data_hoje()

    def serialize(self):

        return json.dumps(self.status)



