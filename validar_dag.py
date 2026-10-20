"""Inspeção estática da DAG verdadeira SEM importar/instalar Airflow.
Valida sintaxe e encontra decorações/tarefas/configuração; não executa o Airflow.
"""
import ast
from pathlib import Path
p=Path(__file__).resolve().parent/'dags'/'pipeline_vendas_airflow3.py'
txt=p.read_text(encoding='utf-8');tree=ast.parse(txt)
sdk=any(isinstance(n,ast.ImportFrom) and n.module=='airflow.sdk' for n in ast.walk(tree))
funcoes=[n for n in ast.walk(tree) if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef))]
decorators=[ast.unparse(d) for f in funcoes for d in f.decorator_list]
config=next((f for f in funcoes if f.name=='pipeline_vendas'),None)
schedule=None;catchup=None
if config:
 for d in config.decorator_list:
  if isinstance(d,ast.Call):
   for kw in d.keywords:
    if kw.arg=='schedule' and isinstance(kw.value,ast.Constant):schedule=kw.value.value
    if kw.arg=='catchup' and isinstance(kw.value,ast.Constant):catchup=kw.value.value
assert sdk and config and schedule and any(d.startswith('task.sensor(') for d in decorators) and 'task' in decorators, 'Estrutura inesperada da DAG'
assert isinstance(schedule,str) and len(schedule.split())==5, 'Schedule cron deve conter 5 campos'
assert catchup is False, 'Este laboratório usa catchup=False'
print('VALIDAÇÃO ESTÁTICA: PASS')
print('Arquivo:',p.name)
print('API real encontrada: airflow.sdk')
print('TaskFlow @task: SIM | @task.sensor: SIM | @dag: SIM')
print('Schedule:', schedule, '| catchup:',catchup)
print('Tarefas:', ', '.join(f.name for f in funcoes if f.name not in ('pipeline_vendas',)))
print('IMPORTANTE: validação de sintaxe/estrutura; não significa que o Airflow foi instalado ou executado.')
