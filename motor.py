import pandas as pd

class MotorRegrasSimulacao:

    def __init__(self, meses_minimos=3, margem_minima=0):
        self.meses_minimos = meses_minimos
        self.margem_minima = margem_minima
        self.hoje = pd.Timestamp.today()

    def regra_tempo_empresa(self, df, coluna='dataAdmissao'):
        datas = pd.to_datetime(df[coluna], format='%d%m%Y', errors='coerce')

        df['regra_tempo_empresa'] = (datas <= self.hoje - pd.DateOffset(months=self.meses_minimos)).astype(int)

        return df

    def regra_pep(self, df, coluna='pessoaExpostaPoliticamente.descricao'):
        df['regra_pep'] = (df[coluna].fillna('').str.strip().str.lower().eq('pessoa não exposta politicamente')).astype(int)

        return df

    def regra_elegibilidade(self, df, coluna='elegivelEmprestimo'):
        df['regra_elegivel'] = df[coluna].astype(bool).astype(int)
        return df

    def regra_margem(self, df, coluna='margemDisponivel'):
        df['regra_margem'] = (df[coluna] >= self.margem_minima).astype(int)
        return df

    def aplicar_regras(self, df):
        df = self.regra_tempo_empresa(df)
        df = self.regra_pep(df)

        df['aprovado_final'] = (
                (df['regra_tempo_empresa'] == 1) &
                (df['regra_pep'] == 1)
        ).astype(int)

        return df