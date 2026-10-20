"""Funções de ETL reais (Python puro), compartilhadas pelo executor didático e pelo DAG Airflow 3."""
from __future__ import annotations
import csv
from pathlib import Path

BASE = Path(__file__).resolve().parent
DADOS = BASE / 'dados'
SAIDA = BASE / 'saida'
SAIDA.mkdir(exist_ok=True)

def extrair_pedidos() -> dict:
    origem = DADOS / 'pedidos.csv'
    destino = SAIDA / 'pedidos_brutos.csv'
    if not origem.exists():
        raise FileNotFoundError(f'Arquivo de entrada não encontrado: {origem}')
    with origem.open('r', encoding='utf-8-sig', newline='') as f:
        registros = list(csv.DictReader(f))
    if not registros:
        raise ValueError('A fonte CSV não possui pedidos')
    with destino.open('w', encoding='utf-8', newline='') as f:
        w=csv.DictWriter(f, fieldnames=['pedido_id','categoria','status','valor']); w.writeheader(); w.writerows(registros)
    return {'arquivo':str(destino), 'pedidos_extraidos':len(registros)}

def transformar_pedidos() -> dict:
    origem=SAIDA/'pedidos_brutos.csv'; destino=SAIDA/'receita_por_categoria.csv'
    with origem.open('r', encoding='utf-8', newline='') as f:
        registros=list(csv.DictReader(f))
    receita={}; aprovados=0
    for r in registros:
        status=r['status'].strip().lower()
        valor=float(r['valor'].replace(',','.'))
        if status=='pago' and valor>0:
            categoria=r['categoria'].strip().title()
            receita[categoria]=receita.get(categoria,0.)+valor
            aprovados+=1
    with destino.open('w',encoding='utf-8',newline='') as f:
        w=csv.writer(f);w.writerow(['categoria','receita']);
        for categoria in sorted(receita): w.writerow([categoria,f'{receita[categoria]:.2f}'])
    return {'arquivo':str(destino),'pedidos_validos':aprovados,'categorias':len(receita)}

def validar_qualidade(forcar_falha=False) -> dict:
    if forcar_falha:
        raise ValueError('Falha didática injetada: qualidade insuficiente (cenário proposital)')
    arquivo=SAIDA/'receita_por_categoria.csv'
    with arquivo.open('r',encoding='utf-8',newline='') as f: linhas=list(csv.DictReader(f))
    if not linhas or any(float(r['receita'])<0 for r in linhas):
        raise ValueError('Teste de qualidade falhou: vazio ou valor negativo')
    return {'resultado':'PASS','categorias_testadas':len(linhas)}

def notificar_sucesso() -> dict:
    relatorio=SAIDA/'notificacao.txt'
    relatorio.write_text('Pipeline concluído e validado. Relatório disponível em receita_por_categoria.csv\n', encoding='utf-8')
    return {'notificacao':str(relatorio)}
