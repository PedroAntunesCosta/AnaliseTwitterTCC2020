
# **********************************************************************************************************************

# Olá!
#
# Esses códigos foram desenvolvidos para o meu trabalho de conclusão de curso do MBA em marketing digital pelo UniCEUB.
# Não sou desenvolvedor, o código não está otimizado e tem muitos problemas. Mas é funcional e atende ao propósito muito bem.
# Esse programa realiza diversas análises sobre os senadores a partir dos dados coletados e do resultado da análise de redes.

# **********************************************************************************************************************

# coding=utf-8

import pandas as pd
import os
import shutil
import sklearn
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import confusion_matrix
from sklearn.model_selection import train_test_split
from sklearn import preprocessing

# Importação dos dados
senadores = pd.DataFrame(pd.read_csv('dados/senadores_votos_previdência.csv'))
_29d1 = pd.DataFrame(pd.read_csv('resultados/Reforma da previdência e senadores REDES/Reforma da previdência e senadores 29d1_Análise de Redes/dados perfis rede_Reforma da previdência e senadores 29d1.csv'))
_29d2 = pd.DataFrame(pd.read_csv('resultados/Reforma da previdência e senadores REDES/Reforma da previdência e senadores 29d2_Análise de Redes/dados perfis rede_Reforma da previdência e senadores 29d2.csv'))
_58d = pd.DataFrame(pd.read_csv('resultados/Reforma da previdência e senadores REDES/Reforma da previdência e senadores 58d_Análise de Redes/dados perfis rede_Reforma da previdência e senadores 58d.csv'))
tweets_58d = pd.DataFrame(pd.read_csv('resultados/Reforma da previdência e senadores REDES/Reforma da previdência e senadores 58d_Análise de Redes/dados tweets rede_Reforma da previdência e senadores 58d.csv'))
tweets_29d1 = pd.DataFrame(pd.read_csv('resultados/Reforma da previdência e senadores REDES/Reforma da previdência e senadores 29d1_Análise de Redes/dados tweets rede_Reforma da previdência e senadores 29d1.csv'))
tweets_29d2 = pd.DataFrame(pd.read_csv('resultados/Reforma da previdência e senadores REDES/Reforma da previdência e senadores 29d2_Análise de Redes/dados tweets rede_Reforma da previdência e senadores 29d2.csv'))

print(tweets_58d.info())

# Seleciona apenas senadores que estão presentes nos principais clusters em ambos os períodos
nos_clusters_bool = []
for senador in senadores['Perfil_senador']:
        dados_senador = _29d1[_29d1['Usuário'] == senador.strip('@')]
        dados_senador_2 = _29d2[_29d2['Usuário'] == senador.strip('@')]
        if dados_senador.shape[0] > 0 and dados_senador_2.shape[0] > 0:
            nos_clusters_bool.append(True)
        else:
            nos_clusters_bool.append(False)

senadores = senadores[nos_clusters_bool]

print(senadores.info())

# --------------------------------------------------------------------------------------------------------------29d1

periodo = '29d1'
print('Período: {}'.format(periodo))

# Listas para acrescentar como colunas ao data frame de senadores

modularidade = []
voto_esperado = []
degree = []
curtidas = []
rts = []
engajamento = []
publi = []

# pega as infos do período

for senador in senadores['Perfil_senador']:
    dados_senador = _29d1[_29d1['Usuário'] == senador.strip('@')]
    modularidade.append(dados_senador['Modularidade'].iloc[0])
    degree.append(dados_senador['Degree'].iloc[0])
    curtidas.append(dados_senador['Curtidas'].iloc[0])
    rts.append(dados_senador['Retweets'].iloc[0])
    engajamento.append(dados_senador['Engajamento'].iloc[0])
    publi.append(dados_senador['Publicações'].iloc[0])
    if dados_senador['Modularidade'].iloc[0] == 0:
        voto_esperado.append('Sim')
    if dados_senador['Modularidade'].iloc[0] == 1:
        voto_esperado.append('Não')

# Das listas para o data frame

senadores['Modularidade {}'.format(periodo)] = modularidade
senadores['Degree {}'.format(periodo)] = degree
senadores['Curtidas {}'.format(periodo)] = curtidas
senadores['Retweets {}'.format(periodo)] = rts
senadores['Engajamento {}'.format(periodo)] = engajamento
senadores['Publicações {}'.format(periodo)] = publi
senadores['Voto esperado {}'.format(periodo)] = voto_esperado

# -----------------------------------------------------------------------------------------------------------29d2

periodo = '29d2'
print('Período: {}'.format(periodo))

# Listas para acrescentar como colunas ao data frame de senadores

modularidade = []
voto_esperado = []
degree = []
curtidas = []
rts = []
engajamento = []
publi = []

# pega as infos do período

for senador in senadores['Perfil_senador']:
    dados_senador = _29d2[_29d2['Usuário'] == senador.strip('@')]
    modularidade.append(dados_senador['Modularidade'].iloc[0])
    degree.append(dados_senador['Degree'].iloc[0])
    curtidas.append(dados_senador['Curtidas'].iloc[0])
    rts.append(dados_senador['Retweets'].iloc[0])
    engajamento.append(dados_senador['Engajamento'].iloc[0])
    publi.append(dados_senador['Publicações'].iloc[0])
    if dados_senador['Modularidade'].iloc[0] == 0:
        voto_esperado.append('Sim')
    if dados_senador['Modularidade'].iloc[0] == 1:
        voto_esperado.append('Não')

# Das listas para o data frame

senadores['Modularidade {}'.format(periodo)] = modularidade
senadores['Degree {}'.format(periodo)] = degree
senadores['Curtidas {}'.format(periodo)] = curtidas
senadores['Retweets {}'.format(periodo)] = rts
senadores['Engajamento {}'.format(periodo)] = engajamento
senadores['Publicações {}'.format(periodo)] = publi
senadores['Voto esperado {}'.format(periodo)] = voto_esperado

# ---------------------------------------------------------------------------------------------------------------58d

periodo = '58d'
print('Período: {}'.format(periodo))

# Listas para acrescentar como colunas ao data frame de senadores

modularidade = []
voto_esperado = []
degree = []
curtidas = []
rts = []
engajamento = []
publi = []

# pega as infos do período

for senador in senadores['Perfil_senador']:
    dados_senador = _58d[_58d['Usuário'] == senador.strip('@')]
    modularidade.append(dados_senador['Modularidade'].iloc[0])
    degree.append(dados_senador['Degree'].iloc[0])
    curtidas.append(dados_senador['Curtidas'].iloc[0])
    rts.append(dados_senador['Retweets'].iloc[0])
    engajamento.append(dados_senador['Engajamento'].iloc[0])
    publi.append(dados_senador['Publicações'].iloc[0])
    if dados_senador['Modularidade'].iloc[0] == 0:
        voto_esperado.append('Sim')
    if dados_senador['Modularidade'].iloc[0] == 1:
        voto_esperado.append('Não')

# Das listas para o data frame

senadores['Modularidade {}'.format(periodo)] = modularidade
senadores['Degree {}'.format(periodo)] = degree
senadores['Curtidas {}'.format(periodo)] = curtidas
senadores['Retweets {}'.format(periodo)] = rts
senadores['Engajamento {}'.format(periodo)] = engajamento
senadores['Publicações {}'.format(periodo)] = publi
senadores['Voto esperado {}'.format(periodo)] = voto_esperado

# ---------------------------------------------------------------------------------------------------------Regressão

print('\n-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/\nREGRESSÃO\n-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-/*\n')

periodos_lista = ['29d1', '29d2', '58d']
variaveis_lista = ['Publicações', 'Curtidas', 'Degree', 'Retweets', 'Tweets sobre previdência', 'Falou sobre previdência']

# Registra as previsões que foram acertos ou erros
for periodo in periodos_lista:
    senadores['Previsão acertada {}'.format(periodo)] = senadores['Voto esperado {}'.format(periodo)] == senadores['Voto']

variaveis_scores = {}
score_minimo = 0.6

print('TESTANDO VARIÁVEIS')
# Testa variáveis
for variavel in variaveis_lista:
    print('\n///////// {}'.format(variavel))
    x_treino, x_teste, y_treino, y_teste = train_test_split(senadores['{} 58d'.format(variavel)].values.reshape(-1, 1), senadores['Previsão acertada 58d'])
    modelo = LogisticRegression()
    modelo.fit(X=x_treino, y=y_treino)
    score = modelo.score(X=x_teste, y=y_teste)
    print('Score: {}'.format(score))
    variaveis_scores[variavel] = score

# modelo com todas as variáveis

x1_treino, x1_teste, x2_treino, x2_teste, x3_treino, x3_teste, x4_treino, x4_teste, x5_treino, \
x5_teste, x6_treino, x6_teste, y_treino, y_teste = train_test_split(
    senadores['Publicações 58d'],
    senadores['Degree 58d'],
    senadores['Curtidas 58d'],
    senadores['Retweets 58d'],
    senadores['Tweets sobre previdência 58d'],
    senadores['Falou sobre previdência 58d'],
    senadores['Previsão acertada 58d']
)

x_treino_df = pd.DataFrame()
x_treino_df['p'] = x1_treino
x_treino_df['d'] = x2_treino
x_treino_df['c'] = x3_treino
x_treino_df['r'] = x4_treino
x_treino_df['t'] = x5_treino
x_treino_df['f'] = x6_treino


x_teste_df = pd.DataFrame()
x_teste_df['p'] = x1_teste
x_teste_df['d'] = x2_teste
x_teste_df['c'] = x3_teste
x_teste_df['r'] = x4_teste
x_teste_df['t'] = x5_teste
x_teste_df['f'] = x6_teste

x_treino = x_treino_df[['p', 'd', 'c', 'r', 't', 'f']]
x_teste = x_teste_df[['p', 'd', 'c', 'r', 't', 'f']]

modelo_multi = LogisticRegression(max_iter=200)
modelo_multi.fit(X=x_treino, y=y_treino)
score = modelo_multi.score(X=x_teste, y=y_teste)
print('\n///////// MODELO COMPLETO')
print('Score: {}'.format(score))

# Probabilidade para cada senador
x_tudo = senadores[['Publicações 58d', 'Degree 58d', 'Curtidas 58d', 'Retweets 58d',
                    'Tweets sobre previdência 58d', 'Falou sobre previdência 58d']]
senadores['Prob voto'] = [x[1] for x in modelo_multi.predict_proba(X=x_tudo)]
print(senadores.sort_values(by='Prob voto').head(15).filter(items=['Senador(a)', 'Prob voto']))

# ----------------------------------------------------------------------------------------------Quem mudou de cluster

mudou_cluster_bool = []
for senador in senadores['Perfil_senador']:
        dados_senador = senadores[senadores['Perfil_senador'] == senador]
        if dados_senador['Modularidade 29d1'].iloc[0] != dados_senador['Modularidade 29d2'].iloc[0]:
            mudou_cluster_bool.append(True)
        else:
            mudou_cluster_bool.append(False)

senadores['Mudou de grupo'] = mudou_cluster_bool
sen_mudou = senadores[mudou_cluster_bool]

print(sen_mudou['Senador(a)'])

# ------------------------------------------------------------------------------------------------Exporta dados e resumo

pasta = 'resultados/análise dos senadores na previdência'

senadores.to_csv('{}/senadores.csv'.format(pasta), index=False)
sen_mudou.to_csv('{}/senadores que mudaram de grupo.csv'.format(pasta), index=False)

# Relatório geral

relatorio_sim = '-------------------------------------------------------------------------------------\nVOTARAM SIM\n\n'
relatorio_nao = '-------------------------------------------------------------------------------------\nVOTARAM NÃO\n\n'

for index, senador in senadores.iterrows():
    rel = '**************{}**************\n' \
          'Voto: {}\n' \
          'Probabilidade calculada de votar com o grupo inicial: {}\n' \
          'Grupo inicial: {}\n' \
          'Grupo final: {}\n' \
          'Grupo geral: {}\n' \
          'Degree geral: {}\n' \
          'Publicações geral: {}\n' \
          'Publicações sobre a reforma da previdência: {}' \
          '\n\n'\
        .format(
        senador['Senador(a)'],
        senador['Voto'],
        senador['Prob voto'],
        senador['Voto esperado 29d1'],
        senador['Voto esperado 29d2'],
        senador['Voto esperado 58d'],
        senador['Degree 58d'],
        senador['Publicações 58d'],
        senador['Tweets sobre previdência']
    )
    if senador['Voto'] == 'Sim':
        relatorio_sim = relatorio_sim + rel
    if senador['Voto'] == 'Não':
        relatorio_nao = relatorio_nao + rel

arquivo_relatorio_geral = open('{}/relatório geral senadores previdência.txt'.format(pasta), 'w', encoding='utf-8')

arquivo_relatorio_geral.write(
    'SENADORES E SUA RELAÇÃO COM O DEBATE DA PREVIDÊNCIA\n\n'
    '{} senadores apareceram nos principais grupos\n'
    'Votaram SIM: {}\n'
    'Votaram NÂO: {}\n'
    'Mudaram de grupo: {}\n{}\n\n'
    '{}\n\n'
    '{}'
        .format(
        senadores.shape[0],
        senadores[senadores['Voto'] == 'Sim'].shape[0],
        senadores[senadores['Voto'] == 'Não'].shape[0],
        sen_mudou.shape[0],
        sen_mudou['Perfil_senador'],
        relatorio_sim,
        relatorio_nao
    )
)

# Relatórios por senador

for index, senador in senadores.iterrows():

    # pasta
    pasta_sen = '{}/{}'.format(pasta, senador['Senador(a)'])
    if os.path.isdir(pasta_sen):
        shutil.rmtree(pasta_sen)
    os.makedirs(pasta_sen)

    # dados
    pub_29d1 = tweets_29d1[tweets_29d1['Usuário'] == senador['Perfil_senador'].strip('@')]
    pub_29d1.to_csv('{}/{}_Publicações 29d1.csv'.format(pasta_sen, senador['Senador(a)']))

    pub_29d2 = tweets_29d2[tweets_29d2['Usuário'] == senador['Perfil_senador'].strip('@')]
    pub_29d2.to_csv('{}/{}_Publicações 29d2.csv'.format(pasta_sen, senador['Senador(a)']))

    pub_58d = tweets_58d[tweets_58d['Usuário'] == senador['Perfil_senador'].strip('@')]
    pub_58d.to_csv('{}/{}_Publicações 58d.csv'.format(pasta_sen, senador['Senador(a)']))

    # relatório
    # período 1
    rel = '{}**************************\n' \
          'Voto: {}\n\n' \
          '=====================================================================\nPERÍODO 1 - {} A {}\n\n' \
          'Grupo: {}\n' \
          'Degree: {}\n' \
          'Publicações: {}\n\n' \
          'Publicações com maior engajamento no período:\n\n' \
        .format(
        senador['Senador(a)'],
        senador['Voto'],
        tweets_29d1['DataPyt'].min(),
        tweets_29d1['DataPyt'].max(),
        senador['Voto esperado 29d1'],
        senador['Degree 29d1'],
        senador['Publicações 29d1'],
    )

    # publicações
    for index, pub in pub_29d1.sort_values(by='Engajamento', ascending=False).head(10).iterrows():
        pub_rel = '{} às {}\n' \
                  '{}\n' \
                  '{}\n' \
                  '{} curtidas | {} retweets\n\n'\
            .format(
            pub['Data'],
            pub['Hora'],
            pub['Url'],
            pub['Texto'],
            pub['Curtidas'],
            pub['Retweets']
        )

        rel = rel + pub_rel

    # período 2
    rel = rel + '=====================================================================\nPERÍODO 2 - {} A {}\n\n' \
          'Grupo: {}\n' \
          'Degree: {}\n' \
          'Publicações: {}\n\n' \
          'Publicações com maior engajamento no período:\n\n' \
        .format(
        tweets_29d2['DataPyt'].min(),
        tweets_29d2['DataPyt'].max(),
        senador['Voto esperado 29d2'],
        senador['Degree 29d2'],
        senador['Publicações 29d2'],
    )

    # publicações
    for index, pub in pub_29d2.sort_values(by='Engajamento', ascending=False).head(10).iterrows():
        pub_rel = '{} às {}\n' \
                  '{}\n' \
                  '{}\n' \
                  '{} curtidas | {} retweets\n\n'\
            .format(
            pub['Data'],
            pub['Hora'],
            pub['Url'],
            pub['Texto'],
            pub['Curtidas'],
            pub['Retweets']
        )

        rel = rel + pub_rel

    # período geral
    rel = rel + '=====================================================================\nPERÍODO GERAL - {} A {}\n\n' \
                'Grupo: {}\n' \
                'Degree: {}\n' \
                'Publicações: {}\n\n' \
                'Publicações com maior engajamento no período:\n\n' \
        .format(
        tweets_58d['DataPyt'].min(),
        tweets_58d['DataPyt'].max(),
        senador['Voto esperado 58d'],
        senador['Degree 58d'],
        senador['Publicações 58d'],
    )

    # publicações
    for index, pub in pub_58d.sort_values(by='Engajamento', ascending=False).head(10).iterrows():
        pub_rel = '{} às {}\n' \
                  '{}\n' \
                  '{}\n' \
                  '{} curtidas | {} retweets\n\n' \
            .format(
            pub['Data'],
            pub['Hora'],
            pub['Url'],
            pub['Texto'],
            pub['Curtidas'],
            pub['Retweets']
        )

        rel = rel + pub_rel

    # salva relatório
    arquivo_relatorio_senador = open('{}/{}.txt'.format(pasta_sen, senador['Senador(a)']), 'w', encoding='utf-8')
    arquivo_relatorio_senador.write(rel)





