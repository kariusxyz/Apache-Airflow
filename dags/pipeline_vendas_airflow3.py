"""DAG REAL para Apache Airflow 3.x. Não executar com `python` sem Airflow.
Projeto raiz deve estar no Python Path do Airflow (ver README)."""
from __future__ import annotations
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from airflow.sdk import PokeReturnValue, dag, task
from tarefas import SAIDA, extrair_pedidos, transformar_pedidos, validar_qualidade, notificar_sucesso

@dag(
    dag_id='semana12_pipeline_vendas',
    schedule='0 3 * * *',  # TODO aluno: altere para '0 6 * * *' (6h)
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args={'retries':2,'retry_delay':timedelta(minutes=1)},
    tags=['semana12','estudo'],
)
def pipeline_vendas():
    @task
    def extrair():
        return extrair_pedidos()

    @task.sensor(poke_interval=10,timeout=120,mode='reschedule')
    def aguardar_arquivo():
        return PokeReturnValue(is_done=(SAIDA/'pedidos_brutos.csv').exists())

    @task
    def transformar():
        return transformar_pedidos()

    @task
    def validar():
        return validar_qualidade()

    @task
    def notificar():
        return notificar_sucesso()

    e=extrair()
    s=aguardar_arquivo()
    t=transformar()
    v=validar()
    n=notificar()
    e >> s >> t >> v >> n

pipeline_vendas()
