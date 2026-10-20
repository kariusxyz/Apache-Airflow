"""Executor didático de estados/ordem/tentativas: NÃO é o Apache Airflow.
Roda as MESMAS funções de negócio da DAG real sem dependências externas.
Uso: python executar_local.py --cenario normal|falha|sensor
"""
from __future__ import annotations
import argparse, json, sys
from datetime import datetime
from pathlib import Path
from tarefas import SAIDA, extrair_pedidos, transformar_pedidos, validar_qualidade, notificar_sucesso

TASKS=['extrair_pedidos','aguardar_arquivo','transformar_pedidos','validar_qualidade','notificar_sucesso']

def executar(cenario='normal', imprimir=True):
    if cenario not in ('normal','falha','sensor'):
        raise ValueError('Cenário inválido')
    SAIDA.mkdir(exist_ok=True)
    # Evita deixar aviso de sucesso antigo durante uma execução que falha.
    aviso_anterior=SAIDA/'notificacao.txt'
    if aviso_anterior.exists(): aviso_anterior.unlink()
    eventos=[];estados={t:'queued' for t in TASKS}
    def log(msg):
        linha=f'[{datetime.now().strftime("%H:%M:%S")}] {msg}'
        eventos.append(linha)
        if imprimir: print(linha)
    log(f'EXECUTOR DIDÁTICO LOCAL (não Airflow) | cenário: {cenario}')
    for i,t in enumerate(TASKS):
        if t=='aguardar_arquivo':
            if not (SAIDA/'pedidos_brutos.csv').exists():
                estados[t]='waiting'; log('SENSOR: aguardando pedidos_brutos.csv');break
            if cenario=='sensor' and not (SAIDA/'sinal.ok').exists():
                estados[t]='waiting';log('SENSOR: aguardando sinal.ok — execute python criar_sinal.py e rode novamente');break
        estados[t]='running';log(f'RUNNING {t}')
        tentativas=3 if t=='validar_qualidade' and cenario=='falha' else 1
        ok=False
        for tentativa in range(1,tentativas+1):
            try:
                if t=='extrair_pedidos': dados=extrair_pedidos()
                elif t=='aguardar_arquivo': dados={'arquivo_detectado': True}
                elif t=='transformar_pedidos': dados=transformar_pedidos()
                elif t=='validar_qualidade': dados=validar_qualidade(forcar_falha=(cenario=='falha'))
                else: dados=notificar_sucesso()
                log(f'SUCCESS {t} | tentativa {tentativa} | {dados}')
                estados[t]='success';ok=True;break
            except Exception as e:
                log(f'ERRO {t} | tentativa {tentativa}/{tentativas} | {type(e).__name__}: {e}')
                if tentativa<tentativas:log(f'RETRY {t} — nova tentativa imediata no laboratório (Airflow real aguardaria retry_delay)')
                else:estados[t]='failed'
        if not ok:break
    for t in TASKS:
        if estados[t]=='queued' and 'failed' in estados.values():estados[t]='upstream_failed'
        elif estados[t]=='queued' and 'waiting' in estados.values():estados[t]='blocked'
    status=('success' if all(x=='success' for x in estados.values()) else 'failed' if 'failed' in estados.values() else 'waiting')
    log(f'ESTADO FINAL: {status.upper()} | '+ ' → '.join(f'{k}:{v}' for k,v in estados.items()))
    rel={'cenario':cenario,'estado_final':status,'estados':estados,'eventos':eventos,'executor':'didatico_local_nao_airflow'}
    (SAIDA/'execucao.json').write_text(json.dumps(rel,ensure_ascii=False,indent=2),encoding='utf-8')
    (SAIDA/'execucao.log').write_text('\n'.join(eventos)+'\n',encoding='utf-8')
    log('Arquivos gerados: saida/execucao.json e saida/execucao.log')
    return rel

if __name__=='__main__':
    p=argparse.ArgumentParser(description='Executa pipeline didático e registra evidências')
    p.add_argument('--cenario',choices=['normal','falha','sensor'],default='normal')
    args=p.parse_args()
    res=executar(args.cenario)
    sys.exit(0 if res['estado_final'] in ['success','waiting'] else 2)
