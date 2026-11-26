"""Gravação na base analítica, via PostgresHook e Connection do Airflow."""

from __future__ import annotations

import logging
from datetime import date

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import get_current_context

logger = logging.getLogger(__name__)

CONN_ID = "postgres_analitico"
FUSO = "America/Sao_Paulo"

DDL = """
CREATE TABLE IF NOT EXISTS precos_por_categoria (
    data_referencia DATE           NOT NULL,
    categoria       TEXT           NOT NULL,
    qtd_produtos    INTEGER        NOT NULL,
    preco_medio     NUMERIC(10, 2) NOT NULL,
    preco_minimo    NUMERIC(10, 2) NOT NULL,
    preco_maximo    NUMERIC(10, 2) NOT NULL,
    atualizado_em   TIMESTAMPTZ    NOT NULL DEFAULT now(),
    PRIMARY KEY (data_referencia, categoria)
);

CREATE TABLE IF NOT EXISTS historico_precos_categoria (
    id              BIGSERIAL      PRIMARY KEY,
    executado_em    TIMESTAMPTZ    NOT NULL DEFAULT now(),
    data_referencia DATE           NOT NULL,
    categoria       TEXT           NOT NULL,
    qtd_produtos    INTEGER        NOT NULL,
    preco_medio     NUMERIC(10, 2) NOT NULL,
    preco_minimo    NUMERIC(10, 2) NOT NULL,
    preco_maximo    NUMERIC(10, 2) NOT NULL
);
"""

# A chave primária (data_referencia, categoria) + ON CONFLICT é o que garante
# a idempotência: rodar a mesma run de novo atualiza a linha, não duplica.
UPSERT_SNAPSHOT = """
INSERT INTO precos_por_categoria (
    data_referencia, categoria, qtd_produtos, preco_medio, preco_minimo, preco_maximo
)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (data_referencia, categoria) DO UPDATE SET
    qtd_produtos  = EXCLUDED.qtd_produtos,
    preco_medio   = EXCLUDED.preco_medio,
    preco_minimo  = EXCLUDED.preco_minimo,
    preco_maximo  = EXCLUDED.preco_maximo,
    atualizado_em = now();
"""

INSERT_HISTORICO = """
INSERT INTO historico_precos_categoria (
    data_referencia, categoria, qtd_produtos, preco_medio, preco_minimo, preco_maximo
)
VALUES (%s, %s, %s, %s, %s, %s);
"""


def data_de_referencia() -> date:
    """Data que identifica a execução — é a chave de idempotência.

    Em run manual o Airflow 3 pode não definir logical_date, então caímos para
    run_after, que existe sempre. Convertido para date puro porque o driver do
    Postgres não sabe adaptar os tipos do pendulum.
    """
    dag_run = get_current_context()["dag_run"]
    momento = dag_run.logical_date or dag_run.run_after
    local = pendulum.instance(momento).in_timezone(FUSO)
    return date(local.year, local.month, local.day)


def criar_tabelas() -> None:
    PostgresHook(postgres_conn_id=CONN_ID).run(DDL)
    logger.info("Tabelas conferidas/criadas na base analítica")


def salvar_snapshot(metricas: list[dict]) -> None:
    """Grava o retrato do dia (upsert por data + categoria)."""
    referencia = data_de_referencia()
    _executar(UPSERT_SNAPSHOT, _linhas(metricas, referencia))
    logger.info(
        "Snapshot de %s gravado: %s categorias", referencia, len(metricas)
    )


def salvar_historico(metricas: list[dict]) -> None:
    """Grava o histórico em append, para acompanhar a evolução dos preços."""
    referencia = data_de_referencia()
    _executar(INSERT_HISTORICO, _linhas(metricas, referencia))
    logger.info(
        "Histórico de %s gravado: %s linhas novas", referencia, len(metricas)
    )


def _linhas(metricas: list[dict], referencia: date) -> list[tuple]:
    return [
        (
            referencia,
            metrica["categoria"],
            metrica["qtd_produtos"],
            metrica["preco_medio"],
            metrica["preco_minimo"],
            metrica["preco_maximo"],
        )
        for metrica in metricas
    ]


def _executar(sql: str, linhas: list[tuple]) -> None:
    """Roda o mesmo comando para todas as linhas, em uma única transação."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conexao = hook.get_conn()
    try:
        with conexao.cursor() as cursor:
            cursor.executemany(sql, linhas)
        conexao.commit()
    finally:
        conexao.close()
