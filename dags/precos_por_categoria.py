"""Pipeline diário de preços por categoria da ShopBrasil.

Substitui o script agendado no cron: busca os produtos no catálogo, calcula
as métricas por categoria e grava na base analítica em PostgreSQL.

Topologias:
    linear   -> buscar_produtos > validar_produtos > agrupar_por_categoria
    fan-out  -> calcular_metricas.expand(), uma task por categoria
    fan-in   -> consolidar_metricas junta o resultado de todas elas
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.sdk import TaskGroup, dag, task

from shopbrasil import alertas, api, metricas, postgres
from shopbrasil.operadores import ValidarProdutosOperator

logger = logging.getLogger(__name__)


@dag(
    dag_id="precos_por_categoria_diario",
    description="Métricas diárias de preço por categoria (FakeStore -> PostgreSQL)",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    tags=["shopbrasil", "precos"],
    default_args={
        "owner": "time-de-dados",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    on_failure_callback=alertas.alerta_falha_dag,
)
def precos_por_categoria_diario():
    @task(
        # Task crítica: a API oscila de madrugada, então aqui a espera entre as
        # tentativas dobra a cada falha (30s, 1min, 2min... até o teto de 5min).
        retries=3,
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=5),
        on_failure_callback=alertas.alerta_falha,
        on_retry_callback=alertas.alerta_retry,
        on_success_callback=alertas.alerta_sucesso,
    )
    def buscar_produtos() -> list[dict]:
        return api.buscar_produtos()

    @task
    def agrupar_por_categoria(produtos: list[dict]) -> list[dict]:
        grupos = metricas.agrupar_por_categoria(produtos)
        logger.info("Categorias encontradas: %s", [g["categoria"] for g in grupos])
        return grupos

    @task(pool="ecommerce_pool")
    def calcular_metricas(grupo: dict) -> dict:
        resultado = metricas.calcular_metricas(grupo)
        logger.info("Métricas de %s: %s", resultado["categoria"], resultado)
        return resultado

    @task
    def consolidar_metricas(por_categoria: list[dict]) -> list[dict]:
        consolidado = sorted(por_categoria, key=lambda metrica: metrica["categoria"])
        logger.info("Consolidadas %s categorias", len(consolidado))
        return consolidado

    @task
    def preparar_tabelas() -> None:
        postgres.criar_tabelas()

    @task
    def salvar_snapshot(consolidado: list[dict]) -> None:
        postgres.salvar_snapshot(consolidado)

    @task
    def salvar_historico(consolidado: list[dict]) -> None:
        postgres.salvar_historico(consolidado)

    with TaskGroup(group_id="ingestao"):
        produtos = buscar_produtos()
        validados = ValidarProdutosOperator(task_id="validar_produtos", produtos=produtos)
        grupos = agrupar_por_categoria(validados.output)

    with TaskGroup(group_id="analise"):
        consolidado = consolidar_metricas(calcular_metricas.expand(grupo=grupos))

    with TaskGroup(group_id="persistencia"):
        tabelas = preparar_tabelas()
        tabelas >> [salvar_snapshot(consolidado), salvar_historico(consolidado)]


precos_por_categoria_diario()
