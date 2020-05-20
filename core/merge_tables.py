from db_tools.read_sqlite import ReadSqlite
from config import PATH_BANCO_HP, APP_TABLES
from .proj_decorators import save_df_to_db


class TableMerger:

    def __init__(self, path_banco = None, conn_banco_app = None):

        path_banco = path_banco or PATH_BANCO_HP
        self.reader = ReadSqlite(path_banco)
        if conn_banco_app:
            self.conn = conn_banco_app

    @save_df_to_db
    def dados_obito(self, **kwargs):

        #baixando tables
        localizacao = self.reader.table_as_df('tb_localizacao')
        falecido = self.reader.table_as_df('tb_falecido')
        cidade = self.reader.table_as_df('tb_cidade')

        #fazendo merges de dados do falecido
        falecido = falecido.merge(localizacao, on='id_falecido', how='left')
        falecido = falecido.merge(cidade, on='id_cidade', suffixes=('', '_cidade'), how='left')

        #deletando tables que nao precisa mais
        del cidade
        del localizacao

        #pegando table de dados do obito
        dados_obito = self.reader.table_as_df('tb_dados_obito')

        #adicionando dados do falecido puxados anteriormente

        dados_obito = dados_obito.merge(falecido, on='id_falecido', how='left')

        #setando o index para ajudar nos merges posteriores
        dados_obito.set_index('id_dados_obito', inplace=True)

        return dados_obito

    @save_df_to_db
    def cemiterios(self, **kwargs):

        #puxando tabelas
        cemiterio = self.reader.table_as_df('tb_cemiterio')
        cidade = self.reader.table_as_df('tb_cidade')

        #fazendo o merge
        cemiterio = cemiterio.merge(cidade, on='id_cidade', how='left', suffixes=('', '_cidade')).copy()

        #setando index
        cemiterio.set_index('id_cemiterio', inplace=True)

        return cemiterio

    @save_df_to_db
    def contratacao(self, **kwargs):

        #puxando tabelas
        contratacao = self.reader.table_as_df('tb_contratacao')
        tipo_destino_final = self.reader.table_as_df('tb_tipo_destino_final')
        tipo_operacao = self.reader.table_as_df('tb_tipo_operacao')
        tb_tipo_contratacao = self.reader.table_as_df('tb_tipo_contratacao')

        #fazendo os merges
        contratacao = contratacao.merge(tipo_destino_final, on='id_tipo_destino_final', how='left').copy()
        contratacao = contratacao.merge(tipo_operacao, on='id_tipo_operacao', how='left').copy()
        contratacao = contratacao.merge(tb_tipo_contratacao, on='id_tipo_contratacao', how='left').copy()

        #setando o index
        contratacao.set_index('id_contratacao', inplace=True)

        return contratacao

    def renomear_cols(self, df, prefix, id_col):

        cols = {col: f'{prefix}_{col}' for col in df.keys() if col != id_col}
        df = df.rename(cols, axis =1).copy()

        return df

    def renomear_tabelas_auxiliares(self, dados_obito, contratacao, cemiterio):
        '''Renomeia tabelas auxiliares para auxiliar leitura dos merges depois'''


        dados_obito = self.renomear_cols(dados_obito, 'obito', 'id_dados_obito')
        contratacao = self.renomear_cols(contratacao, 'contratacao', 'id_contratacao')
        cemiterio = self.renomear_cols(cemiterio, 'cemiterio', 'id_cemiterio')

        return dados_obito, contratacao, cemiterio

    @save_df_to_db
    def sepultamento(self, contratacao, cemiterios, dados_obito, **kwargs):


        #renomeia_tabelas para evitar dificuldades de leitura
        dados_obito, contratacao, cemiterios = self.renomear_tabelas_auxiliares(dados_obito, contratacao, cemiterios)

        #puxando dados
        sepult = self.reader.table_as_df('tb_sepultamento')

        #fazendo os merges
        sepult = sepult.merge(contratacao, on='id_contratacao', how='left').copy()
        sepult = sepult.merge(cemiterios, on='id_cemiterio', how='left').copy()
        sepult = sepult.merge(dados_obito, on='id_dados_obito', how='left').copy()


        return sepult

    @save_df_to_db
    def cremacao(self, contratacao, cemiterios, dados_obito, **kwargs):

        # renomeia_tabelas para evitar dificuldades de leitura
        dados_obito, contratacao, cemiterios = self.renomear_tabelas_auxiliares(dados_obito, contratacao, cemiterios)

        #puxando dados
        cremacao = self.reader.table_as_df('tb_cremacao')

        #fazendo os merges
        cremacao = cremacao.merge(contratacao, on='id_contratacao', how='left')
        cremacao = cremacao.merge(cemiterios, on='id_cemiterio', how='left').copy()
        cremacao = cremacao.merge(dados_obito, on='id_dados_obito', how='left').copy()

        return cremacao

    def gerar_todas_tables(self, save = False):

        #toda tabela na lista_tables deve ter um metodo de nome correspondente nesse objeto

        dados_obito = self.dados_obito(tb_name = 'dados_obito', save_as_sqlite = save)
        cemiterios = self.cemiterios(tb_name = 'cemiterios', save_as_sqlite = save)
        contratacao = self.contratacao(tb_name = 'contratacao', save_as_sqlite = save)
        sepultamento = self.sepultamento(contratacao, cemiterios, dados_obito,
                                         tb_name = 'sepultamento', save_as_sqlite = save)
        cremacao = self.cremacao(contratacao, cemiterios, dados_obito,
                                 tb_name = 'cremacao', save_as_sqlite = save)

        dfs = {}
        dfs['cremacao'] = cremacao
        dfs['sepultamento']  = sepultamento
        dfs['cemiterios'] = cemiterios
        dfs['contratacao'] = contratacao
        dfs['dados_obito'] = dados_obito


        return dfs



