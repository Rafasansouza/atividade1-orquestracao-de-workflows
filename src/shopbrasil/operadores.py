"""Operador customizado de validação de schema."""

from __future__ import annotations

from airflow.exceptions import AirflowException
from airflow.sdk import BaseOperator

CAMPOS_OBRIGATORIOS = ("id", "titulo", "categoria", "preco")


class ValidarProdutosOperator(BaseOperator):
    """Confere o schema dos produtos antes de calcular qualquer métrica.

    `produtos` está em template_fields: por isso dá para passar o retorno da
    task anterior direto na chamada (`produtos=buscar_produtos()`). O Airflow
    resolve o XCom em tempo de execução e já cria a dependência entre as tasks.
    """

    template_fields = ("produtos",)

    def __init__(self, produtos, **kwargs) -> None:
        super().__init__(**kwargs)
        self.produtos = produtos

    def execute(self, context) -> list[dict]:
        invalidos = [
            f"produto {produto.get('id')}: {motivo}"
            for produto in self.produtos
            if (motivo := self._primeiro_problema(produto))
        ]

        if invalidos:
            raise AirflowException(
                "Produtos fora do schema esperado -> " + "; ".join(invalidos)
            )

        self.log.info("%s produtos validados", len(self.produtos))
        return self.produtos

    @staticmethod
    def _primeiro_problema(produto: dict) -> str | None:
        ausentes = [campo for campo in CAMPOS_OBRIGATORIOS if produto.get(campo) is None]
        if ausentes:
            return f"campos ausentes {ausentes}"
        if not isinstance(produto["preco"], (int, float)):
            return "preço não é numérico"
        if produto["preco"] <= 0:
            return "preço menor ou igual a zero"
        if not str(produto["categoria"]).strip():
            return "categoria vazia"
        return None
