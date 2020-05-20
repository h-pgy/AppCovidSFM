from flask import Blueprint, render_template, current_app, request, send_file, make_response, redirect, url_for, flash
from app.status_handler import StatusHandler
from app.helpers import headers_no_chache
from app.paralel_update import paralel_update
from core.file_handle import FileHandler
from core.proj_exceptions import EmAtualizacao
import os


page_extrair = Blueprint('page_extrair', __name__,
                        template_folder='templates')


@page_extrair.before_request
def check_if_atualizando():
    status = StatusHandler(current_app)
    if status.banco_em_atualizacao:
        flash("Em atualização!")

@page_extrair.route('/microdados')
def microdados():

    status = StatusHandler(current_app)
    return render_template('microdados.html', status = status)

@page_extrair.route('/downloads')
def download():
    status = StatusHandler(current_app)
    if status.banco_em_atualizacao:
        return redirect(url_for('page_extrair.microdados'))
    try:
        tb_name = request.args.get('table')
        file_handler = FileHandler(status)
        file_handler.check_atualizada(tb_name)
        path_app = os.path.split(__file__)[0]
        f_path = os.path.join(path_app, f'static/files/{tb_name}.xlsx')
        print(f_path)
        r = make_response(send_file(f_path, attachment_filename=f'{tb_name}.xlsx'))
        # abaixo coloco headers para o browser nao cachear as files
        headers_no_chache(r)

        return r

    except EmAtualizacao:
        return redirect(url_for('page_extrair.microdados'))

@page_extrair.route('/microdados/app_status')
def app_status():
    status = StatusHandler(current_app)
    return status.serialize()

@page_extrair.route('/atualizar')
def atualizar():
    status = StatusHandler(current_app)
    if status.banco_em_atualizacao:
        return redirect(url_for('page_extrair.microdados'))
    paralel_update(status)

    return redirect(url_for('page_extrair.microdados'))





