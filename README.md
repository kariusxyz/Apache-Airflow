# Semana 12 — Apache Airflow (atividade autônoma de 80 minutos)

## Como começar no VS Code — Windows / macOS / Linux

1. Extraia o ZIP. **Não rode os scripts dentro do ZIP.**
2. No VS Code: **Arquivo → Abrir Pasta** e selecione `projeto_aluno`.
3. Abra **Terminal → Novo Terminal**. O terminal deve estar na pasta que contém `executar_local.py`.
4. Em Windows com Python instalado, rode `py validar_ambiente.py`; em macOS/Linux, `python3 validar_ambiente.py`. Se `python` funciona, pode usar `python` em todos os exemplos.
5. Seguir **caderno HTML** da semana. Não precisa Docker, WSL, conta ou pip para o laboratório local.

## Comandos usados (se `python` não funcionar, use `py` no Windows)

```sh
python validar_ambiente.py
python executar_local.py --cenario normal
python executar_local.py --cenario falha
python executar_local.py --cenario sensor
python criar_sinal.py
python executar_local.py --cenario sensor
python validar_dag.py
python -m unittest discover -s testes -v
```

Após executar, veja `saida/receita_por_categoria.csv`, `saida/execucao.log` e `saida/execucao.json`.

**Importante:** o arquivo `executar_local.py` é um **executor pedagógico em Python puro**, e não o Airflow. Ele executa as mesmas funções de negócio de um exemplo de DAG e simula estados, dependências e retry para a turma sem instalar serviços. O arquivo `dags/pipeline_vendas_airflow3.py` é uma **DAG verdadeira escrita para a API pública do Apache Airflow 3** (`airflow.sdk`). `python validar_dag.py` inspeciona sintaxe/estrutura dessa DAG, mas **não instala nem inicia o Airflow real**.

### Fluxo e estados

`extrair_pedidos → aguardar_arquivo → transformar_pedidos → validar_qualidade → notificar_sucesso`

- **normal**: todos `success`, escreve receita por categoria.
- **falha**: validação falha 3 vezes (execução inicial + `retries=2`); notificação fica `upstream_failed`. A simulação faz retries imediatos para economizar tempo; Airflow real espera `retry_delay`.
- **sensor**: sem `saida/sinal.ok`, permanece `waiting`; rode `python criar_sinal.py`, depois repita o sensor e veja `success`.

## Atenção ao teste automático

O comando `python -m unittest discover -s testes -v` verifica cenários normal, falha, sensor e sintaxe da DAG; ao terminar, pode deixar o arquivo `saida/execucao.json` com o último cenário **sensor**. Isto é esperado.

## Modo Airflow de verdade (opcional, depois da aula)

A DAG está pronta para ser adicionada a uma instalação Airflow 3 **em Linux/WSL2**, com o diretório raiz disponível para o ambiente Airflow. Consulte a documentação oficial https://airflow.apache.org/docs/apache-airflow/stable/start.html e a documentação de autoria https://airflow.apache.org/docs/apache-airflow/stable/public-airflow-interface.html. A instalação completa não cabe com segurança nos 80 minutos da aula autônoma e não deve ser exigida para esta entrega.

## Problemas comuns

- **`python` não reconhecido**: Windows tente `py`; macOS/Linux tente `python3`.
- **Arquivo não encontrado**: extraiu ZIP? Terminal está dentro da pasta do projeto (`executar_local.py` aparece no explorador)?
- **Erro do PowerShell**: não precisa ativar venv, nem executar políticas ou scripts `.ps1`.
- **`ModuleNotFoundError: airflow` ao abrir DAG**: não execute o arquivo da DAG diretamente. Para inspeção sem Airflow use `python validar_dag.py`.
- **`--cenario falha` retorna código de saída 2**: previsto. O teste de qualidade falha de propósito; faça print do terminal.
- **`--cenario sensor` não aguarda**: apague `saida/sinal.ok`, se existir, e rode de novo. Só faça isso no laboratório.
- **PDF sem imagens**: antes de salvar, verifique se os prints foram anexados ao caderno e use “Salvar/Imprimir PDF”; todas as seções entram na impressão automaticamente. Ative “Gráficos de plano de fundo” se desejar cores.
- **Backup MD**: contém texto e nomes/legendas das evidências, **não carrega os arquivos de imagem**. O JSON salva as imagens embutidas e permite importação.
