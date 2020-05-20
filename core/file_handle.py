import os
from core.DAO import DAO
import datetime

class FileHandler:

    def __init__(self, status, tempo_maximo = 120):

        self.status = status
        self.max_time = tempo_maximo
        self.DAO = DAO(app_status = status)
        self.path_files = r'C:\Users\h-pgy\Desktop\COVID\AppCovidSFM/app/static/files'

    def make_files_dir(self):

        if not os.path.exists(self.path_files) or not os.path.isdir(self.path_files):
            os.makedirs(self.path_files)

    def dt_status_to_dtime(self, data_atualiz):

        return datetime.datetime.strptime(data_atualiz,
                                          "%b, %d %Y %H:%M:%S")

    def dif_atualiz(self, data_atualiz):

        agora = datetime.datetime.now()

        data_atualiz = self.dt_status_to_dtime(data_atualiz)

        dif = agora - data_atualiz

        return dif.total_seconds()/60

    def salvar_file(self, tb_name):

        df = self.DAO.retornar_df(tb_name)
        self.make_files_dir()
        file_path = os.path.join(self.path_files, tb_name + '.xlsx')
        df.to_excel(file_path)
        self.status.atualizar_file(tb_name)
        print(f'Arquivo {file_path} salvo')

    def checar_atualiz_banco(self, atualiz_file):

        atualiz_file = self.dt_status_to_dtime(atualiz_file)
        atualiz_banco = self.status.ultima_atualizacao_banco
        atualiz_banco = self.dt_status_to_dtime(atualiz_banco)

        dif = atualiz_file - atualiz_banco

        return dif.total_seconds() > 0

    def checar_atualiz_file(self, atualiz_file):
        #descontinuei isso - nao precisa atualizar se banco nao atualizou

        dif = self.dif_atualiz(atualiz_file)

        return dif < self.max_time


    def check_atualizada(self, tb_name):

        file_mdata = self.status.file_atualizada(tb_name)
        if not file_mdata['Criada']:
            self.salvar_file(tb_name)
        else:
            atualiz_file = file_mdata['Dt_criacao']
            if not self.checar_atualiz_banco(atualiz_file):
                file_mdata['Atualizada'] = False
                self.salvar_file(tb_name)
                file_mdata['Atualizada'] = True
            else:
                pass #file está ok nao precisa salvar mais