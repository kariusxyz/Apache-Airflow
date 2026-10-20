import sys
from pathlib import Path
print('SEMANA 12 — DIAGNÓSTICO')
print('Python:',sys.version.split()[0])
print('Pasta do projeto:', Path(__file__).resolve().parent)
print('Fonte existe:', (Path(__file__).parent/'dados'/'pedidos.csv').exists())
print('Bibliotecas externas necessárias: NENHUMA para laboratório local')
print('O Apache Airflow real é opcional e não está embutido no executor didático.')
assert sys.version_info >= (3,9), 'Python 3.9+ é necessário'
print('AMBIENTE: PASS')
