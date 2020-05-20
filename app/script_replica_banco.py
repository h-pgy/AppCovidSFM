if __name__ == "__main__":

    from config import PATH_BANCO_HP, PATH_BANCO_APP
    from core.data_updater import Updater
    u = Updater(path_banco_hp = PATH_BANCO_HP, path_banco_app = PATH_BANCO_APP)
    u.update_all()