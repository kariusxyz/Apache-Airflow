import ast, contextlib, io, json, sys, tempfile, unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from tarefas import SAIDA
from executar_local import executar

class TestFluxo(unittest.TestCase):
    def test_normal(self):
        x=executar('normal',False)
        self.assertEqual(x['estado_final'],'success')
        self.assertEqual(set(x['estados'].values()),{'success'})
        self.assertTrue((SAIDA/'receita_por_categoria.csv').exists())
    def test_falha_e_retries(self):
        x=executar('falha',False)
        self.assertEqual(x['estado_final'],'failed')
        self.assertEqual(x['estados']['notificar_sucesso'],'upstream_failed')
        self.assertEqual(len([e for e in x['eventos'] if 'ERRO validar_qualidade' in e]),3)
    def test_sensor(self):
        sinal=SAIDA/'sinal.ok'
        if sinal.exists():sinal.unlink()
        x=executar('sensor',False)
        self.assertEqual(x['estado_final'],'waiting')
        self.assertEqual(x['estados']['aguardar_arquivo'],'waiting')
        sinal.write_text('OK',encoding='utf-8')
        self.assertEqual(executar('sensor',False)['estado_final'],'success')
        sinal.unlink()
    def test_dag_sintaxe(self):
        ast.parse((ROOT/'dags'/'pipeline_vendas_airflow3.py').read_text(encoding='utf-8'))

if __name__=='__main__':unittest.main(verbosity=2)
