from flask import Flask
from app.extrator_dados_bp import page_extrair
from app.status_handler import StatusHandler
from config import S_KEY


def create_app():

    app = Flask(__name__)
    app.secret_key = S_KEY
    app.config.status = {}
    status = StatusHandler(app)
    status.init_status()
    app.register_blueprint(page_extrair)

    return app

