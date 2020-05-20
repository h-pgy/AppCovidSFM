from core.data_updater import Updater
import threading

def paralel_update_func(status):

    status.banco_em_atualizacao_setter(True)
    updater = Updater()
    print('iniciando update em paralelo')
    updater.update_all()
    print('finalizou atualizacao')
    status.banco_em_atualizacao_setter(False)
    status.ultima_atualizacao_banco_setter()

def paralel_update(status):
    x = threading.Thread(target=paralel_update_func, args = [status,])
    x.start()