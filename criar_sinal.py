from tarefas import SAIDA
(SAIDA/'sinal.ok').write_text('OK: dados disponíveis\n',encoding='utf-8')
print('Sinal criado: saida/sinal.ok. Agora execute python executar_local.py --cenario sensor')
