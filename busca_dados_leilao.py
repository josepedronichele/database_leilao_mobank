import requests
import pandas as pd
import time
import json
import os
from datetime import datetime
import pytz

from motor import MotorRegrasSimulacao
from sqlalchemy import create_engine, text


# ================= CONFIG VIA ENV =================

TOKEN_API_FIXO = os.getenv("TOKEN_API_FIXO")
DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)

# ==================================================


def formatar_data_api(dt):
    return dt.strftime("%d%m%Y%H%M%S")


def obter_data_inicio():
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT MAX(dataHoraValidadeSolicitacao)
            FROM dataprev_solicitacoes
        """))
        ultima = result.scalar()

    if ultima:
        return ultima

    return formatar_data_api(datetime.now())


def buscar_solicitacoes_trabalhador(token_api, data_hora_inicio, data_hora_fim, delay=1, max_tentativas=5):

    url = "https://monbank.co/api/dataprev/propostas/solicitacoes-trabalhador"

    headers = {
        "accept": "application/json",
        "Authorization": token_api,
        "X-CSRF-TOKEN": ""
    }

    pagina = 0
    todas_solicitacoes = []
    ultimo_hash_pagina = None

    while True:
        params = {
            "nroPagina": pagina,
            "dataHoraInicio": data_hora_inicio,
            "dataHoraFim": data_hora_fim
        }

        tentativa = 0
        sucesso = False

        while tentativa < max_tentativas and not sucesso:
            try:
                response = requests.get(url, headers=headers, params=params, timeout=60)
                response.raise_for_status()
                dados = response.json()

                lista = dados.get("content") or dados.get("data") or dados

                if not lista:
                    return todas_solicitacoes

                hash_pagina = hash(json.dumps(lista, sort_keys=True))

                if hash_pagina == ultimo_hash_pagina:
                    return todas_solicitacoes

                ultimo_hash_pagina = hash_pagina

                todas_solicitacoes.extend(lista)

                pagina += 1
                sucesso = True
                time.sleep(delay)

            except Exception:
                tentativa += 1
                time.sleep(delay * tentativa)

        if not sucesso:
            break

    return todas_solicitacoes


def salvar_no_postgres(df):

    if df.empty:
        print("DataFrame vazio.")
        return

    df.columns = df.columns.str.replace('.', '_', regex=False)

    fuso = pytz.timezone("America/Sao_Paulo")
    df["hora_atualizacao"] = datetime.now(fuso)

    registros = df.to_dict(orient="records")

    sql = """
    INSERT INTO dataprev_solicitacoes (
        idSolicitacao,
        cpf,
        matricula,
        numeroInscricaoEmpregador,
        valorLiberado,
        nroParcelas,
        dataHoraValidadeSolicitacao,
        nomeTrabalhador,
        dataNascimento,
        margemDisponivel,
        elegivelEmprestimo,
        dataAdmissao,
        inscricaoEmpregador_codigo,
        inscricaoEmpregador_descricao,
        pessoaExpostaPoliticamente_codigo,
        pessoaExpostaPoliticamente_descricao,
        regra_tempo_empresa,
        regra_pep,
        aprovado_final,
        hora_atualizacao
    )
    VALUES (
        :idSolicitacao,
        :cpf,
        :matricula,
        :numeroInscricaoEmpregador,
        :valorLiberado,
        :nroParcelas,
        :dataHoraValidadeSolicitacao,
        :nomeTrabalhador,
        :dataNascimento,
        :margemDisponivel,
        :elegivelEmprestimo,
        :dataAdmissao,
        :inscricaoEmpregador_codigo,
        :inscricaoEmpregador_descricao,
        :pessoaExpostaPoliticamente_codigo,
        :pessoaExpostaPoliticamente_descricao,
        :regra_tempo_empresa,
        :regra_pep,
        :aprovado_final,
        :hora_atualizacao
    )
    ON CONFLICT (idSolicitacao) DO NOTHING;
    """

    with engine.begin() as conn:
        conn.execute(text(sql), registros)

    print(f"{len(registros)} registros enviados.")


def executar():

    data_inicio = obter_data_inicio()
    data_fim = formatar_data_api(datetime.now())

    print(f"Buscando de {data_inicio} até {data_fim}")

    dados = buscar_solicitacoes_trabalhador(
        token_api=TOKEN_API_FIXO,
        data_hora_inicio=data_inicio,
        data_hora_fim=data_fim
    )

    if not dados:
        print("Nenhum dado encontrado.")
        return

    df_temp = pd.json_normalize(dados)

    motor = MotorRegrasSimulacao(meses_minimos=3, margem_minima=0)
    df = motor.aplicar_regras(df_temp)

    salvar_no_postgres(df)

    print(f"{len(df)} registros processados.")


if __name__ == "__main__":

    print("Worker iniciado...")

    while True:
        try:
            executar()
        except Exception as e:
            print("Erro na execução:", e)

        print("Aguardando 60 segundos...\n")
        time.sleep(60)
