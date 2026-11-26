"""Agrupamento e cálculo das métricas de preço por categoria."""

from __future__ import annotations

from collections import defaultdict


def agrupar_por_categoria(produtos: list[dict]) -> list[dict]:
    """Quebra a lista de produtos em um item por categoria.

    As categorias saem dos próprios dados. É isso que faz o pipeline escalar
    sozinho: categoria nova na API vira uma task mapeada a mais, sem mexer
    no código.
    """
    por_categoria: dict[str, list[dict]] = defaultdict(list)
    for produto in produtos:
        por_categoria[produto["categoria"]].append(produto)

    return [
        {"categoria": categoria, "produtos": itens}
        for categoria, itens in sorted(por_categoria.items())
    ]


def calcular_metricas(grupo: dict) -> dict:
    """Calcula as métricas de uma única categoria."""
    precos = [float(produto["preco"]) for produto in grupo["produtos"]]

    return {
        "categoria": grupo["categoria"],
        "qtd_produtos": len(precos),
        "preco_medio": round(sum(precos) / len(precos), 2),
        "preco_minimo": round(min(precos), 2),
        "preco_maximo": round(max(precos), 2),
    }
