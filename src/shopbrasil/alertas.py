"""Callbacks de ciclo de vida.

Aqui o alerta é só um log com prefixo, para dar para acompanhar pela UI.
Em produção, o corpo destas funções chamaria Slack, e-mail ou PagerDuty —
a assinatura e o ponto de engate no DAG seriam os mesmos.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def _identificacao(context) -> str:
    ti = context["task_instance"]
    return f"{ti.dag_id}.{ti.task_id} | run {ti.run_id} | tentativa {ti.try_number}"


def alerta_falha(context) -> None:
    """on_failure_callback: a task estourou todas as tentativas."""
    logger.error(
        "[ALERTA] Falha em %s | erro: %s",
        _identificacao(context),
        context.get("exception"),
    )


def alerta_retry(context) -> None:
    """on_retry_callback: a task falhou e vai tentar de novo."""
    logger.warning(
        "[ALERTA] Retry em %s | erro: %s",
        _identificacao(context),
        context.get("exception"),
    )


def alerta_sucesso(context) -> None:
    """on_success_callback: a task terminou bem."""
    logger.info("[OK] Sucesso em %s", _identificacao(context))


def alerta_falha_dag(context) -> None:
    """on_failure_callback do DAG: avisa que a execução inteira falhou."""
    dag_run = context["dag_run"]
    logger.error(
        "[ALERTA] O DAG %s falhou na run %s", dag_run.dag_id, dag_run.run_id
    )
