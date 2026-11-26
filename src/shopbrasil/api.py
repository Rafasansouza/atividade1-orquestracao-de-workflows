"""Cliente da API de catálogo (FakeStore)."""

from __future__ import annotations

import logging

import requests

logger = logging.getLogger(__name__)

URL_PRODUTOS = "https://fakestoreapi.com/products"
TIMEOUT_SEGUNDOS = 15


class FalhaNaAPI(Exception):
    """Erro ao conversar com a API de catálogo."""


def buscar_produtos() -> list[dict]:
    """Busca os produtos e devolve só os campos usados pelo pipeline.

    O retorno vira XCom, então não vale a pena carregar o payload inteiro
    (descrição e imagem de cada produto). Ficam id, título, categoria e preço.

    Qualquer problema de rede, status HTTP ou JSON inválido vira FalhaNaAPI,
    que marca a task como falha e deixa o Airflow acionar o retry.
    """
    try:
        resposta = requests.get(URL_PRODUTOS, timeout=TIMEOUT_SEGUNDOS)
        resposta.raise_for_status()
        payload = resposta.json()
    except (requests.RequestException, ValueError) as erro:
        raise FalhaNaAPI(f"Não foi possível buscar os produtos: {erro}") from erro

    if not payload:
        raise FalhaNaAPI("A API respondeu com uma lista vazia de produtos")

    produtos = [
        {
            "id": item.get("id"),
            "titulo": item.get("title"),
            "categoria": item.get("category"),
            "preco": item.get("price"),
        }
        for item in payload
    ]

    logger.info("Recebidos %s produtos da API", len(produtos))
    return produtos
