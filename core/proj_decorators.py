def save_df_to_db(func):

    def wrapped(*args, **kwargs):

        df = func(*args, **kwargs)
        salvar_em_banco = kwargs.get('save_as_sqlite')
        if salvar_em_banco:
            try:
                obj = args[0]
                con_banco = getattr(obj, 'conn', None) or kwargs['conn']
            except KeyError:
                raise RuntimeError('must pass a db connection, either throw self.conn or as conn kwargs')
            try:
                tb_name = kwargs['tb_name']
            except KeyError:
                raise RuntimeError('tb_name parameter must be defined when saving to db')
            df.to_sql(tb_name, con_banco, if_exists = 'replace')

        return df

    return wrapped

