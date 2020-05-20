import sqlite3
import mysql.connector

class ReplicadorMySql:

    def __init__(self, conn_dic_mysql, nome_banco):

        self.mysql = self.conectar_mysql(**conn_dic_mysql)
        self.sqlite = self.conectar_sqlite(nome_banco)

    def pegar_colunas(self, cursor, table_name, cols = None):

        cursor.execute(f"SHOW columns FROM {table_name}")
        col_data =  cursor.fetchall()

        if cols:

            col_data = (col for col in col_data if col[0] in cols)

        return col_data

    def parsear_colunas(self, lista_colunas):

        def parse_tipo(tipo):

            tipo = tipo.lower().strip()
            if 'int' in tipo:
                return 'INTEGER'
            if ('datetime' in tipo) or ('date' in tipo) or ('timestamp' in tipo):
                return 'TEXT'
            if 'varchar' in tipo:
                return 'TEXT'
            if ('float' in tipo) or (tipo == 'double') or ('decimal' in tipo):
                return 'REAL'
            if tipo == 'text' or tipo == 'longtext' or tipo == 'mediumtext' or tipo.startswith('char'):
                return 'TEXT'
            else:
                print(f'Tipo {tipo} nao previsto, parseado como texto')
                return 'TEXT'

        col_tuplas = [(col[0], parse_tipo(col[1])) for col in lista_colunas]

        return col_tuplas

    def pegar_dados(self, cursor, callback):

        while True:
            row = cursor.fetchone()
            if row:
                callback(row)
            else:
                break

    def parsear_dados(self, rows):

        parsed = []
        for row in rows:
            temp_row = []
            for item in row:
                if type(item) == str:
                    item = item.replace('"', '')
                    item = item.replace("'", '')
                    temp_row.append(f'"{item}"')
                elif type(item) == int or type(item) == float:
                    temp_row.append(f'{item}')
                elif type(item) == bool:
                    temp_row.append(str(item).upper())
                elif item is None:
                    temp_row.append('NULL')
                else:
                    temp_row.append(f'"{item}"')
            value_row = "( " + ','.join(temp_row) + ' )'
            parsed.append(value_row)

        return ',\n'.join(parsed)

    def dropar_tabela(self, cnx, table_name):

        query = f'''DROP TABLE IF EXISTS {table_name};'''
        cursor = cnx.cursor()
        cursor.execute(query)
        cnx.commit()
        cursor.close()

    def criar_tabela(self, cnx, table_name, col_tuplas):

        self.dropar_tabela(cnx, table_name)
        cursor = cnx.cursor()
        q1 = f"CREATE TABLE IF NOT EXISTS {table_name}"
        q2 = '(' + ' ,\n '.join([f'{t[0]} {t[1]}' for t in col_tuplas]) + ')'

        query = q1 + q2

        cursor.execute(query)
        cnx.commit()
        cursor.close()

    def insert_query(self, cnx, table_name, rows, cols = None):

        if cols:
            col_names = ','.join([col[0] for col in cols])
            query = f'INSERT INTO {table_name} ({col_names}) VALUES' + rows
        else:
            query = f'INSERT INTO {table_name} VALUES' + rows
        try:
            cursor = cnx.cursor()
            cursor.execute(query)
            cnx.commit()
            cursor.close()
        except Exception as e:
            print(query[:100])
            raise e

    def conectar_mysql(self, user, password, host, port, db):

        con = mysql.connector.connect(user=user, password=password,
                                      host=host,
                                      port=port,
                                      db=db
                                      )
        return con

    def conectar_sqlite(self, nome_banco):

        con = sqlite3.connect(nome_banco)

        return con

    def puxar_tabela(self, con, tb_name, cols = None, where_clause = ''):

        cursor = con.cursor()

        if cols:
            cols_select = ','.join(cols)
            cursor.execute(f'SELECT {cols_select} FROM {tb_name} {where_clause}')
        else:
            cursor.execute(f'SELECT * FROM {tb_name} {where_clause}')

        dados = []
        self.pegar_dados(cursor, dados.append)
        colunas = self.pegar_colunas(cursor, tb_name, cols)

        cursor.close()

        parsed_cols = self.parsear_colunas(colunas)
        parsed_data = self.parsear_dados(dados)

        return parsed_data, parsed_cols


    def replicar_tabela(self, mysql, sqlite, tb_name, cols, where_clause=''):

        dados, cols = self.puxar_tabela(mysql, tb_name, cols, where_clause)
        self.criar_tabela(sqlite, tb_name, cols)
        if dados:
            self.insert_query(sqlite, tb_name, dados, cols)

            print(f'Tabela {tb_name} replicada')
        else:
            print(f'Tabela {tb_name} vazia')


    def listar_tabelas(self, con):

        cursor = con.cursor()
        cursor.execute('SHOW TABLES;')
        tbls = cursor.fetchall()
        cursor.close()
        return [tbl[0] for tbl in tbls]

    def __call__(self, table_mdata=None):

        if table_mdata is None:
            lista_tabelas = self.listar_tabelas(self.mysql)
            table_mdata = {table_name : [] for table_name in lista_tabelas}

        try:
            for tbl, cols in table_mdata.items():
                try:
                    self.replicar_tabela(self.mysql, self.sqlite, tbl, cols)

                except Exception as e:
                    print(f'Erro na tabela {tbl}')
                    print(repr(e))

        finally:
            self.mysql.close()
            self.sqlite.close()

