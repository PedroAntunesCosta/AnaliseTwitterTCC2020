
# **********************************************************************************************************************

# Olá!
#
# Esses códigos foram desenvolvidos para o meu trabalho de conclusão de curso do MBA em marketing digital pelo UniCEUB.
# Não sou desenvolvedor, o código não está otimizado e tem muitos problemas. Mas é funcional e atende ao propósito muito bem.
# Esse programa processa o produto da análise de redes para extrair informações sobre o comportamento dos grupos.
# É similar à primeira análise, mas com mais informações e análise de alguns aspectos dos senadores também.

# **********************************************************************************************************************

import pandas as pd
import seaborn as sns
from datetime import datetime
import matplotlib.pyplot as plt
import urllib.request
import os
import shutil
import unidecode

# ----------------------------------------------------------------------------------------------------- VARIÁVEIS CHAVE


# Define quantidade de usuários que serão selecionados do topo das listas por mais engajamento e mais
# volume de publicações
quantidade_top_lista = 10

# Define quantos tweets serão selecionados nas listas dos que mais engajaram
quantidade_top_tweets = 10

# Define quantos tweets serão selecionados nas listas dos que mais engajaram
quantidade_top_tweets_interesse = 5

# Gerar corpus geral ao fazer a análise?
gerando_corpus = False

# Gerar corpus unificado ao fazer a análise?
gerando_corpus_unificado = False

# Ajuda a definir escopo do tempo, pode ser modificado pelo txt de parâmetros
analisando_tudo = True

# Fazer análise de tema de interesse?
analisando_tema_interesse = False

# Pegar imagens postadas pelos perfis?
coletando_imagens = False

# Gerar imagem das publicações por dia?
gera_imagem_por_dia = False

lista_links_top_interesse = ''


# -------------------------------------------------------------------------------------------------------------FUNÇÕES

# Salva lista de top usuários por engajamento


def salva_top_usuarios(dados, tema_n):
    dados.to_csv('resultados/análises 2/{}/TopUsuáriosEngajamento.csv'.format(tema_n), index=False)


# Salva tweets dos top usuários por engajamento


def salva_tweets_usuarios_engaj(dados, tema_n):
    dados.to_csv('resultados/análises 2/{}/TopUsuáriosEngajamentoTweets.csv'.format(tema_n), index=False)


# Salva lista de top usuários por volume


def salva_top_usuarios_vol(dados, tema_n):
    dados.to_csv('resultados/análises 2/{}/TopUsuáriosVolume.csv'.format(tema_n), index=False)


# Salva tweets dos top usuários por engajamento


def salva_tweets_usuarios_vol(dados, tema_n):
    dados.to_csv('resultados/análises 2/{}/TopUsuáriosVolumeTweets.csv'.format(tema_n), index=False)


# Salva volume de tweets por dia


def salva_tweets_por_dia(dados, tema_n):
    dados.to_csv('resultados/análises 2/{}/TweetsPorDia.csv'.format(tema_n), index=False)


# Salva figura do volume de tweets por dia


def salva_tweets_por_dia_figura(fig, tema_n):
    fig.figure.savefig('resultados/análises 2/{}/TweetsPorDia.png'.format(tema_n), index=False)


# Salva df com todos tweets do tema


def salva_tweets_por_perfil(dados, tema_n):
    dados.to_csv('resultados/análises 2/{}/TweetsNoTema.csv'.format(tema_n), index=False)


# Abre arquivo do corpus


def abre_corpus(tema_n):
    arquivo_corpus = open('resultados/análises 2/{}/análise_de_texto/CorpusTudo.txt'.format(tema_n), 'w',
                          encoding='utf-8')

    return arquivo_corpus


# Abre arquivo do corpus do tema de interesse


def abre_corpus_interesse(tema_n):
    arquivo_corpus = open('resultados/análises 2/{}/análise_de_texto/CorpusInteresse.txt'.format(tema_n),
                          'w', encoding='utf-8')

    return arquivo_corpus


# Abre arquivo do corpus unificado


def abre_corpus_unificado():
    arquivo_corpus = open('resultados/análises 2/CorpusUnificado.txt', 'w', encoding='utf-8')

    return arquivo_corpus


# Abre arquivo do corpus sem RT


def abre_corpus_unificado_srt():
    arquivo_corpus = open('resultados/análises 2/CorpusUnificadoSemRT.txt', 'w', encoding='utf-8')

    return arquivo_corpus


# Abre arquivo do corpus unificado sobre tema de interesse


def abre_corpus_unificado_interesse():
    arquivo_corpus = open('resultados/análises 2/CorpusUnificadoTemaDeInteresse.txt', 'w', encoding='utf-8')

    return arquivo_corpus


# Abre arquivo do corpus unificado sobre tema de interesse com links


def abre_corpus_unificado_interesse_links(tema):
    arquivo_corpus = open('resultados/análises 2/{} - texto e links.txt'.format(tema), 'w', encoding='utf-8')

    return arquivo_corpus


# Abre arquivo do corpus sem RT


def abre_corpus_srt(tema_n):
    arquivo_corpus = open('resultados/análises 2/{}/análise_de_texto/CorpusSemRT.txt'.format(tema_n), 'w',
                          encoding='utf-8')

    return arquivo_corpus


# Abre arquivo do relatório


def abre_relatorio(tema_n):
    arquivo_relatorio = open('resultados/análises 2/{}/Relatório.txt'.format(tema_n), 'w', encoding='utf-8')

    return arquivo_relatorio


# Abre arquivo com links pros tweets com o tema de interesse


def abre_arquivo_links_interesse(tema_n):
    arquivo_links = open('resultados/análises 2/{}/Links.txt'.format(tema_n),
                         'w', encoding='utf-8')

    return arquivo_links


# Salva df com hashtags e contagem de vezes que aparece


def salva_hashtags_contagem(dados, tema_n):
    dados.to_csv('resultados/análises 2/{}/HashtagsContagem.csv'.format(tema_n), index=False)


# Salva df com hashtags e contagem de vezes que aparece


def salva_mentions_contagem(dados, tema_n):
    dados.to_csv('resultados/análises 2/{}/MentionsContagem.csv'.format(tema_n), index=False)


# Salva df com replys e contagem de vezes que aparece


def salva_replys_contagem(dados, tema_n):
    dados.to_csv('resultados/análises 2/{}/ReplysContagem.csv'.format(tema_n), index=False)


# Salva df com tipo de mídia e contagem de vezes que aparece


def salva_media_contagem(dados, tema_n):
    dados.to_csv('resultados/análises 2/{}/MídiaContagem.csv'.format(tema_n), index=False)


# ---------------------------------------------------------------------------------------------------INÍCIO

# Marca tempo inicial
t_inicial = datetime.now()

# Cria colunas do data frame de comparação
comp_id = []
comp_tema = []
comp_perfil = []
comp_url = []
comp_seguidores = []
comp_tweets = []
comp_tweets_srt = []
comp_tweets_dia_media_srt = []
comp_rts_percentual = []
comp_replys_percentual = []
comp_engajamento_medio = []
comp_engajamento_desvpad = []
comp_truncados_tx = []
comp_truncados_tx_srt = []
comp_top_hashtags = []
comp_top_mentions = []
comp_top_replys = []
comp_imagens = []

if analisando_tema_interesse:
    comp_tweets_tema = []
    comp_engajamento_tema = []
    comp_engajamento_percentual_do_geral = []
    comp_engajamento_tema_desvpad = []

# Cria data frame de hashtags geral
total_hashtags = pd.DataFrame()

# Cria data frame de mentions geral
total_mentions = pd.DataFrame()

# Cria data frame de replys geral
total_replys = pd.DataFrame()

# Cria diretório geral dos resultados
if os.path.isdir('resultados/análises 2'):
    shutil.rmtree('resultados/análises 2')
os.makedirs('resultados/análises 2')

# Cria lista de temas
arquivo_de_palavras_chave = open('AnalisarTemas.txt')
lista_temas = []

for linha in arquivo_de_palavras_chave:
    lista_temas.append(linha)

# Cria pasta por dia e apstas numeradas para cada perfil analisado
os.makedirs('resultados/análises 2/por dia')
loop_cria_pastas = 0
for tema in lista_temas:
    loop_cria_pastas += 1

    os.makedirs('resultados/análises 2/{}'.format(loop_cria_pastas))
    os.makedirs('resultados/análises 2/{}/análise_de_texto'.format(loop_cria_pastas))
    os.makedirs('resultados/análises 2/{}/imagens'.format(loop_cria_pastas))
    os.makedirs('resultados/análises 2/{}/hashtags'.format(loop_cria_pastas))
    os.makedirs('resultados/análises 2/{}/mentions'.format(loop_cria_pastas))
    os.makedirs('resultados/análises 2/{}/mentions/senadores'.format(loop_cria_pastas))
    os.makedirs('resultados/análises 2/{}/replys'.format(loop_cria_pastas))
    os.makedirs('resultados/análises 2/{}/datas'.format(loop_cria_pastas))
    os.makedirs('resultados/análises 2/{}/top100'.format(loop_cria_pastas))

arquivo_de_palavras_chave.close()

print('Temas: {}'.format(lista_temas))

# Importa dados coletados
dados = pd.read_csv('dados/prev e prevsen_comunidades_para análise 2.csv')
senadores = pd.read_csv('dados/senadores_votos_previdência.csv')
senadores['Falou sobre previdência 58d'] = 0
senadores['Tweets sobre previdência 58d'] = 0
senadores['Entrou no tema previdênica 58d'] = 'Na'

# Lê arquivo com parâmetros de análise e define variáveis
arquivo_de_parametros = open('ParâmetrosDeAnálise.txt')
parametros_linhas = []
for linha in arquivo_de_parametros:
    parametros_linhas.append(linha)

parametros = []
print('\n -----------------\nParâmetros\n')
for linha in parametros_linhas[14:26]:
    parametro = linha.split('=')[1]
    parametro = parametro[1:].strip('\n')
    parametros.append(parametro)

# Define variáveis de data
data_inicio_str = parametros[0]
data_fim_str = parametros[1]

if data_inicio_str == 'Tudo':
    analisando_tudo = True
    print('Data:\n analisando todo o escopo disponível\n')
else:
    data_is = data_inicio_str.split('/')
    data_fs = data_fim_str.split('/')

    data_inicio = datetime(int(data_is[2]), int(data_is[1]), int(data_is[0])).strftime('%Y-%m-%d')
    data_fim = datetime(int(data_fs[2]), int(data_fs[1]), int(data_fs[0])).strftime('%Y-%m-%d')

    print('Data:\nde {} a {}\n'.format(data_inicio, data_fim))

if analisando_tema_interesse:
    # Define variáveis de termos analisados
    tema_de_interesse = parametros[2]
    palavras_de_interesse = parametros[3].split(',')

else:
    tema_de_interesse = 'NENHUM'
    palavras_de_interesse = 'NENHUMA'

print('\nTema de interesse: {}'.format(tema_de_interesse))
print('Palavras-chave de interesse: {}\n'.format(palavras_de_interesse))


# -----------------------------------------------------------------------------------------------MANIPULA DF

# Cria df sem retweets
dados_srt = dados[dados['Retwitado invert']]

# Faz análises e exporta bases processadas para cada tema
tema_n = 0

# Abre arquivo do índice de temas
indice_temas = open('resultados/análises 2/ÍndiceTemas.txt', 'w')

# filtra df pelo tema de interesse
possui_termo_g = dados['Texto'].apply(lambda tweet:
                                      any(palavra.lower() in tweet.lower()
                                          for palavra in palavras_de_interesse))
dados_interesse_g = dados[possui_termo_g]

# Abre corpus unificado
if gerando_corpus_unificado:
    arquivo_corpus_unificado = abre_corpus_unificado()
    arquivo_corpus_unificado_srt = abre_corpus_unificado_srt()
    arquivo_corpus_unificado_interesse = abre_corpus_unificado_interesse()
    arquivo_corpus_unificado_interesse_links = abre_corpus_unificado_interesse_links(tema_de_interesse)

corpus_unificado = ''
corpus_unificado_srt = ''
corpus_unificado_interesse = ''

# Anotações no relatório da análise
corpus_unificado_interesse_links = 'Texto e links dos tuítes sobre {}\n\n'.format(tema_de_interesse)

corpus_unificado_interesse_links = corpus_unificado_interesse_links + 'Palavras-chave:\n\n'
for palavra in palavras_de_interesse:
    corpus_unificado_interesse_links = corpus_unificado_interesse_links + '- {}\n'.format(palavra)

corpus_unificado_interesse_links = corpus_unificado_interesse_links + '\nTemas analisados:\n\n'
for usuario in lista_temas[0:-1]:
    corpus_unificado_interesse_links = corpus_unificado_interesse_links + '- {}\n'.format(usuario.strip('\n'))

corpus_unificado_interesse_links = corpus_unificado_interesse_links + '\nTexto e links:\n'

# cria df por dia unificado
por_dia_unificado_interesse = pd.DataFrame()

# -----------------------------------------------------------------------------------------------COMEÇA O LOOP POR TEMA

for tema in lista_temas:

    tema_n += 1

    tema = tema.strip('\n')

    nao_publicou = False

    t_inicial_tema = datetime.now()

    if gerando_corpus_unificado:
        corpus_unificado = corpus_unificado + '\n\n\n**** *{}\n\n'.format(tema)
        corpus_unificado_srt = corpus_unificado_srt + '\n\n\n**** *{}\n\n'.format(tema)
        corpus_unificado_interesse = corpus_unificado_interesse + '\n\n\n**** *{}\n\n'.format(tema)

    # Filtra tema/perfil
    dados_tema = dados[dados['Tema'] == tema]
    dados_tema_srt = dados_srt[dados_srt['Tema'] == tema]

    print('--------------Grupo analisado {}: *{}*'.format(tema_n, tema))

    # O que acontece se não tem publicações no período selecionado
    if dados_tema.shape[0] == 0:
        print('Nenhuma publicação no período')
        arquivo_nao_publicou = open('resultados/análises 2/{}/Nenhuma publicação no período.txt'.format(tema_n), 'w')
        arquivo_nao_publicou.write('Nenhuma publicação no período.\nNão há dados para analisar')
        indice_temas.write('Tema {} - {} - Nenhuma publicação no período\n\n'.format(tema_n, tema.strip('\n')))
        continue

    engajamento_medio = dados_tema_srt['Engajamento'].mean()
    desvio_padrao_engajamento = dados_tema_srt['Engajamento'].std()

    # Calcula taxa de truncados
    truncados = dados_tema[dados_tema['Truncado']]
    # Calcula taxa apenas se há truncados
    if dados_tema.shape[0] > 0:
        taxa_de_truncados = truncados.shape[0] / dados_tema.shape[0] * 100
    else:
        taxa_de_truncados = 0

    truncados_srt = dados_tema_srt[dados_tema_srt['Truncado']]
    # Calcula taxa apenas se há truncados
    if dados_tema_srt.shape[0] > 0:
        taxa_de_truncados_srt = truncados_srt.shape[0] / dados_tema_srt.shape[0] * 100
    else:
        taxa_de_truncados_srt = 0

    # Conta RTs
    quantidade_rts = dados_tema.shape[0] - dados_tema_srt.shape[0]
    # Calcula taxa só se houver tuítes sobre o tema
    if dados_tema.shape[0] > 0:
        taxa_rts = quantidade_rts / dados_tema.shape[0]
    else:
        taxa_rts = 0

    # Conta replys
    quantidade_replys = dados_tema[pd.notna(dados_tema['Reply'])].shape[0]
    # Calcula taxa só se houver tuítes sobre o tema
    if dados_tema.shape[0] > 0:
        taxa_replys = quantidade_replys / dados_tema.shape[0]
    else:
        taxa_replys = 0

    # -------------------------------------------------------------------------------------------COLETA IMAGENS

    if coletando_imagens:

        imagens_coletadas = 0
        lista_imagens = []
        metadados_imagens = pd.DataFrame(columns=['Imagem', 'Usuário', 'Data', 'Hora', 'RT', 'Interesse'])

        for index, tweet in dados_tema.iterrows():

            if 'photo' in tweet['Mídia']:

                try:

                    imagens_coletadas += 1

                    lista_imagens.append(imagens_coletadas)

                    imagem_rt = not tweet['Retwitado invert']
                    imagem_interesse = tweet['UrlTema'] in dados_tema['UrlTema']

                    url_imagem = tweet['Mídia Url'].strip('[').strip(']').strip("'")
                    nome_imagem = 'resultados/análises 2/{}/imagens/{}.jpg'.format(tema_n, imagens_coletadas)

                    urllib.request.urlretrieve(url_imagem, nome_imagem)

                    t_metadados_imagens = pd.DataFrame([[int(imagens_coletadas), tweet['Usuário'],
                                                         tweet['Data'], tweet['Hora'], imagem_rt,
                                                         imagem_interesse]],
                                                       columns=['Imagem', 'Usuário', 'Data', 'Hora', 'RT', 'Interesse'])
                    metadados_imagens = pd.concat([metadados_imagens, t_metadados_imagens], ignore_index=True)

                except Exception as erro:

                    print('Tentei jpg salvar mas não deu\n')
                    print(erro)
                    print('\n\n')

            else:

                lista_imagens.append('-')

                print('Ao todo salvei: {} imagens'.format(imagens_coletadas))

        dados_tema['Imagem_ID'] = lista_imagens

        metadados_imagens.to_csv('resultados/análises 2/{}/imagens/MetadadosImagens.csv'.format(tema_n), index=False)

        print('------------imagem ID {}'.format(dados_tema['Imagem_ID']))

    salva_tweets_por_perfil(dados_tema, tema_n)

    # Ajusta datas para efeito de anotação correta depois
    data_inicio = dados_tema['DataPyt'].min()
    data_fim = dados_tema['DataPyt'].max()

    print(len(dados_tema['Seguidores']))

    # Cria str para inserir lista de links depois
    lista_links = ''

    # ---------------------------------------------------------------------------------------------------ENGAJAMENTO

    # Pega usuários com mais engajamento
    top_usuarios_engaj = dados_tema_srt.filter(['Usuário', 'Engajamento'], axis=1).groupby('Usuário',
                                                                                           as_index=False).sum().sort_values(
        by='Engajamento', ascending=False)
    top_usuarios_engaj = top_usuarios_engaj.head(quantidade_top_lista)

    salva_top_usuarios(top_usuarios_engaj, tema_n)

    # Cria df apenas com tweets de usuários na lista com mais engajamento
    tweets_usarios_engaj = pd.DataFrame()
    for usuario in top_usuarios_engaj['Usuário']:
        is_usuario = dados_tema_srt['Usuário'] == usuario
        tweets_usuario = dados_tema_srt[is_usuario]
        tweets_usarios_engaj = pd.concat([tweets_usarios_engaj, tweets_usuario], ignore_index=True)

    # Salva csv dos tweets dos usuários com mais engajamento no período, tirando algumas colunas inúteis
    salva_tweets_usuarios_engaj(tweets_usarios_engaj, tema_n)

    # ---------------------------------------------------------------------------------------------------VOLUME

    # Pega usuários que mais publicaram
    top_usuarios_vol = dados_tema_srt['Usuário'].value_counts(ascending=False)
    top_usuarios_vol = top_usuarios_vol.rename_axis('Usuário').reset_index(name='Volume')
    top_usuarios_vol = top_usuarios_vol.head(quantidade_top_lista)

    salva_top_usuarios_vol(top_usuarios_vol, tema_n)

    # Cria df apenas com tweets de usuários na lista com mais volume
    tweets_usarios_vol = pd.DataFrame()
    for usuario in top_usuarios_vol['Usuário']:
        is_usuario = dados_tema_srt['Usuário'] == usuario
        tweets_usuario = dados_tema_srt[is_usuario]
        tweets_usarios_vol = pd.concat([tweets_usarios_vol, tweets_usuario], ignore_index=True)

    salva_tweets_usuarios_vol(tweets_usarios_vol, tema_n)

    # -------------------------------------------------------------------------------------------------POR DIA 1

    # Publicações por dia com rt
    por_dia = dados_tema.sort_values('DataPyt').filter(items=['DataPyt', 'Url']).groupby('DataPyt', as_index=False) \
        .count()
    por_dia.rename(columns={'DataPyt':'Data', 'Url':'Publicações'}, inplace=True)

    # Publicações por dia sem rt
    # Cria df com as quantidades por dia
    por_dia_srt = pd.DataFrame(columns=['Data', 'Frequência'])
    por_dia_srt['Data'] = dados_tema['DataPyt']
    por_dia_srt['Frequência'] = dados_tema['Retwitado invert']
    por_dia_srt = por_dia_srt.groupby(by='Data', as_index=False).sum()

    # Acrescenta no df por dia a versão em int (sem isso tava indo str)
    por_dia['Publicações (sem RT)'] = por_dia_srt['Frequência'].apply(lambda x: int(x))

    # Calcula número médio de publicações por dia
    tweets_dia_media = round(dados_tema_srt.shape[0] / por_dia.shape[0], 2)

    # -------------------------------------------------------------------------------------------------POR DIA GRÁFICO

    if gera_imagem_por_dia:
        # Gráfico de tweets ao longo do tempo (por dia)
        sns.set_style('darkgrid')
        plt.figure(figsize=(10, 5))
        por_dia_fig = sns.lineplot(x='Data', y='Publicações', data=por_dia)
        por_dia_fig = sns.lineplot(x='Data', y='Publicações (sem RT)', data=por_dia)

        # Ajusta legenda
        plt.legend(['Publicações', 'Sem RT'])

        # Ajusta labels
        por_dia_fig.set_xticklabels(labels=por_dia_fig.get_xticklabels(), rotation=45, horizontalalignment='right')

        # Salva imagem e CSV
        salva_tweets_por_dia_figura(por_dia_fig, tema_n)

        print('\n*****FIGURA OK*****\n')

        # Limpa figura
        plt.clf()

    # ------------------------------------------------------------------------------------------------------DATAS

    print('-*/-*/-*/DATAS-*/-*/-*/')

    for data_ok in dados_tema['DataPyt'].unique():

        dados_data = dados_tema[dados_tema['DataPyt'] == data_ok].sort_values(by='Engajamento', ascending=False)

        rel_data = 'PRINCIPAIS TWEETS EM {}\n\n'.format(data_ok)
        conta_t_data = 0

        for index, tweet in dados_data.head(20).iterrows():
            conta_t_data += 1
            rel_data = rel_data + '{}\n' \
                                '{} em {} às {}\n' \
                                '{}\n' \
                                '{}\n' \
                                '{} Curtidas | {} Retweets\n' \
                                '\n\n'.format(
                conta_t_data,
                tweet['Usuário'],
                tweet['Data'],
                tweet['Hora'],
                tweet['Url'],
                tweet['Texto'],
                tweet['Curtidas'],
                tweet['Retweets']
            )

        dados_data = dados_tema_srt[dados_tema_srt['DataPyt'] == data_ok].sort_values(by='Engajamento', ascending=False)

        rel_data = rel_data + '-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-\n\nORIGINAIS {}\n\n'.format(data_ok)
        conta_t_data = 0

        for index, tweet in dados_data.head(20).iterrows():
            conta_t_data += 1
            rel_data = rel_data + '{}\n' \
                                  '{} em {} às {}\n' \
                                  '{}\n' \
                                  '{}\n' \
                                  '{} Curtidas | {} Retweets\n' \
                                  '\n\n'.format(
                conta_t_data,
                tweet['Usuário'],
                tweet['Data'],
                tweet['Hora'],
                tweet['Url'],
                tweet['Texto'],
                tweet['Curtidas'],
                tweet['Retweets']
            )

        # senadores nessa data

        rel_data = rel_data + '\n========================================================\nPUBLICAÇÕES DE SENADORES\n\n'

        for senador in senadores['Perfil_senador']:
            dados_sena_data = dados_data[dados_data['Usuário'] == senador.strip('@')]

            if dados_sena_data.shape[0] > 0:
                rel_data = rel_data + '------  {}  ------\n\n'.format(senador)

                conta_t_data_sena = 0
                for index, tweet in dados_sena_data.iterrows():
                    conta_t_data_sena += 1
                    rel_data = rel_data + '{}\n' \
                                          '{} em {} às {}\n' \
                                          '{}\n' \
                                          '{}\n' \
                                          '{} Curtidas | {} Retweets\n' \
                                          '\n\n'.format(
                        conta_t_data_sena,
                        tweet['Usuário'],
                        tweet['Data'],
                        tweet['Hora'],
                        tweet['Url'],
                        tweet['Texto'],
                        tweet['Curtidas'],
                        tweet['Retweets']
                    )

        arquivo_rel_data = open('resultados/análises 2/{}/datas/{}.txt'.format(tema_n, data_ok), 'w', encoding='utf-8')
        arquivo_rel_data.write(rel_data)
        arquivo_rel_data.close()
    # ---------------------------------------------------------------------------------------------------SENADORES

    print('-*/-*/-*/-*-*/ SENADORES -*/-*/-/*\n')

    # Marcar se o senador(a) falou sobre previdência e qual foi o primeiro dia

    por_dia['Senador(a) entrou no assunto'] = 'Na'
    por_dia['Senadores entraram no assunto'] = 0
    por_dia['Publicações de senadores no assunto'] = 0
    por_dia['Senadores publicaram no assunto'] = 0

    contador = 0
    for senador in senadores['Perfil_senador']:
        dados_senador = dados_tema[dados_tema['Usuário'] == senador.strip('@')]

        if dados_senador.shape[0] > 0:

            senadores.iloc[contador, senadores.columns.get_loc('Falou sobre previdência 58d')] = 1
            senadores.iloc[contador, senadores.columns.get_loc('Tweets sobre previdência 58d')] = dados_senador.shape[0]
            senadores.iloc[contador, senadores.columns.get_loc('Entrou no tema previdênica 58d')] = dados_senador[
                'DataPyt'].min()

            conta_data = 0
            for dia in por_dia['Data']:
                # Confere se o senador publicou no dia
                dados_senador_dia = dados_senador[dados_senador['DataPyt'] == dia]
                if dados_senador_dia.shape[0] > 0:
                    # Se publicou, incrementa o número de senadores que publicou nesse dia e número de publicações de
                    # senadores do dia
                    por_dia.iloc[conta_data, por_dia.columns.get_loc('Senadores publicaram no assunto')] += 1
                    por_dia.iloc[conta_data, por_dia.columns.get_loc('Publicações de senadores no assunto')] += \
                    dados_senador_dia.shape[0]

                # Marca o senador no primeiro dia em que publicou alguma coisa
                if dia == dados_senador['DataPyt'].min():
                    por_dia.iloc[conta_data, por_dia.columns.get_loc('Senadores entraram no assunto')] += 1

                    if por_dia.iloc[conta_data, por_dia.columns.get_loc('Senador(a) entrou no assunto')] == 'Na':
                        por_dia.iloc[
                            conta_data, por_dia.columns.get_loc('Senador(a) entrou no assunto')] = senador

                    else:
                        por_dia.iloc[
                            conta_data, por_dia.columns.get_loc('Senador(a) entrou no assunto')] += ',' + senador

                conta_data += 1

        # pega mentions dos senadores e faz relatório
        possui_mention_sen = dados_tema['Menções'].str.contains(senador) == True
        mentions_senador = dados_tema[possui_mention_sen].sort_values(by='Engajamento', ascending=False).head(20)
        possui_mention_sen_srt = dados_tema_srt['Menções'].str.contains(senador) == True
        mentions_senador_srt = dados_tema_srt[possui_mention_sen_srt].sort_values(by='Engajamento', ascending=False).head(20)

        rel_mention_sen = '{} Menções ao perfil {}\n======================\n'.format(mentions_senador.shape[0], senador)
        rel_mention_sen = rel_mention_sen + 'TOP 20\n'

        # relatório com tweets importantes da mention no período
        # Com RTs

        conta_mention_sen = 0
        for index, tweet in mentions_senador.iterrows():
            conta_mention_sen += 1
            rel_mention_sen = rel_mention_sen + '{}\n' \
                                        '{} em {} às {}\n' \
                                        '{}\n' \
                                        '{}\n' \
                                        '{} Curtidas | {} Retweets\n' \
                                        '\n\n'.format(
                conta_mention_sen,
                tweet['Usuário'],
                tweet['Data'],
                tweet['Hora'],
                tweet['Url'],
                tweet['Texto'],
                tweet['Curtidas'],
                tweet['Retweets']
            )
        # Sem RTs

        rel_mention_sen = rel_mention_sen + '\n==================================\nTOP 20 ORIGINAIS\n\n'.format(senador)
        conta_mention_sen = 0
        for index, tweet in mentions_senador_srt.iterrows():
            conta_mention_sen += 1
            rel_mention_sen = rel_mention_sen + '{}\n' \
                                        '{} em {} às {}\n' \
                                        '{}\n' \
                                        '{}\n' \
                                        '{} Curtidas | {} Retweets\n' \
                                        '\n\n'.format(
                conta_mention_sen,
                tweet['Usuário'],
                tweet['Data'],
                tweet['Hora'],
                tweet['Url'],
                tweet['Texto'],
                tweet['Curtidas'],
                tweet['Retweets']
            )

        arquivo_rel_mention_sen = open('resultados/análises 2/{}/mentions/senadores/Principais menções_{}.txt'.format(tema_n, senador), 'w', encoding='utf-8')
        arquivo_rel_mention_sen.write(rel_mention_sen)
        arquivo_rel_mention_sen.close()

        contador += 1

    print(por_dia['Senadores entraram no assunto'].unique())
    senadores.to_csv('dados/senadores_votos_previdência.csv', index=False)

    # -------------------------------------------------------------------------------------------GERAÇÃO DE CORPUS GERAL

    if gerando_corpus:
        # Gera corpus com os tweets
        arquivo_corpus = abre_corpus(tema_n)
        corpus = ''
        loop = 0
        for index, tweet in dados_tema.iterrows():
            loop += 1
            corpus = corpus + '{}\n\n'.format(tweet['Texto'])

            if gerando_corpus_unificado:
                corpus_unificado = corpus_unificado + '{}\n\n'.format(tweet['Texto'])

        # Gera corpus sem RT
        arquivo_corpus_srt = abre_corpus_srt(tema_n)
        corpus_srt = ''
        loop_srt = 0
        for index, tweet in dados_tema_srt.iterrows():
            loop_srt += 1
            corpus_srt = corpus_srt + '{}\n\n'.format(tweet['Texto'])

            if gerando_corpus_unificado:
                corpus_unificado_srt = corpus_unificado_srt + '{}\n\n'.format(tweet['Texto'])

        # Salva arquivos com corpus
        arquivo_corpus.write(corpus)
        arquivo_corpus_srt.write(corpus_srt)

    # ----------------------------------------------------------------------------------------------TEMA DE INTERESSE

    if analisando_tema_interesse:
        # Cria df só com dados de tweets que mencionam alguma palavra-chave do tema de interesse
        possui_termo = dados_tema['Texto'].apply(lambda tweet:
                                                 any(palavra.lower() in tweet.lower()
                                                     for palavra in palavras_de_interesse))
        dados_tema_interesse = dados_tema[possui_termo]

        print('----- {} tuítes com termos de interesse'.format(dados_tema_interesse.shape[0]))

        if gerando_corpus_unificado:
            if dados_tema_interesse.shape[0] > 0:
                corpus_unificado_interesse_links = corpus_unificado_interesse_links + '\n\n\n**** *{}\n\n'.format(tema)

        # Gera corpus com tweets que contém palavras-chave do tema de interesse | Anota links no txt de links
        arquivo_corpus_interesse = abre_corpus_interesse(tema_n)
        corpus_interesse = ''
        loop_interesse = 0
        for index, tweet in dados_tema_interesse.sort_values('DataPyt').iterrows():
            loop_interesse += 1

            lista_links = lista_links + '{} - {} às {}'.format(str(loop_interesse), tweet['Data'],
                                                               tweet['Hora'])

            lista_links = lista_links + '\nhttps://{}\n\n'.format(tweet['Url'])

            corpus_interesse = corpus_interesse + '{}\n\n'.format(tweet['Texto'])

            if gerando_corpus_unificado:
                corpus_unificado_interesse = corpus_unificado_interesse + '{}\n\n'.format(tweet['Texto'])
                corpus_unificado_interesse_links = corpus_unificado_interesse_links + '--------------\n'
                corpus_unificado_interesse_links = corpus_unificado_interesse_links + '{} - {} às {}\n'.format(
                    loop_interesse, tweet['Data'], tweet['Hora'])
                corpus_unificado_interesse_links = corpus_unificado_interesse_links + 'https://{}\n'.format(
                    tweet['Url'])
                corpus_unificado_interesse_links = corpus_unificado_interesse_links + '{}\n\n'.format(tweet['Texto'])

        # Cria df com as quantidades por dia
        termo_interesse_por_dia = pd.DataFrame(columns=['Data', 'Frequência'])
        termo_interesse_por_dia['Data'] = dados_tema['DataPyt']
        termo_interesse_por_dia['Frequência'] = possui_termo
        termo_interesse_por_dia = termo_interesse_por_dia.groupby(by='Data', as_index=False).sum()

        # Acrescenta no df por dia a versão em int (sem isso tava indo str)
        por_dia[tema_de_interesse] = termo_interesse_por_dia['Frequência'].apply(lambda x: int(x))

        # Salva arquivos com corpus de interesse
        arquivo_corpus_interesse.write(corpus_interesse)
        if gerando_corpus:
            arquivo_corpus.write('\n**** * {}\n\n'.format(tema_de_interesse) + corpus_interesse)

        dados_tema_interesse_srt = dados_tema_interesse[dados_tema_interesse['Retwitado invert']]
        engajamento_medio_interesse = dados_tema_interesse_srt['Engajamento'].mean()
        desvio_padrao_engajamento_interesse = dados_tema_interesse_srt['Engajamento'].std()
        percentual_engajamento_medio = (engajamento_medio_interesse / engajamento_medio) * 100

        # Engajamento no tema de interesse por dia
        # Cria df com as quantidades por dia
        engajamento_interesse_por_dia = pd.DataFrame(columns=['Data', 'Engajamento'])
        engajamento_interesse_por_dia['Data'] = dados_tema['DataPyt']
        engajamento_interesse_por_dia['Engajamento'] = dados_tema['Engajamento'][possui_termo]
        engajamento_interesse_por_dia = engajamento_interesse_por_dia.groupby(by='Data', as_index=False).sum()

        # Acrescenta no df por dia
        por_dia['Engajamento sobre {}'.format(tema_de_interesse)] = engajamento_interesse_por_dia['Engajamento'] \
            .apply(lambda x: int(x))

    # ---------------------------------------------------------------------------------------------------ENGAJAMENTO 2
    # Esta parte seleciona os tuítes com mais engajamento do perfil

    top_tweets_engajamento = dados_tema_srt.sort_values(by='Engajamento', ascending=False).head(quantidade_top_tweets)

    lista_links_top = '-------------\n{} tuítes (sem RTs) com mais engajamento\n\n'.format(quantidade_top_tweets)

    loop_top = 0

    for index, tweet in top_tweets_engajamento.iterrows():
        loop_top += 1

        lista_links_top = lista_links_top + '{}° - {} às {}'.format(str(loop_top), tweet['Data'], tweet['Hora'])

        lista_links_top = lista_links_top + '\nhttps://{}\n\n'.format(tweet['Url'])

    # Agora com menos engajamento

    bottom_tweets_engajamento = dados_tema_srt.sort_values(by='Engajamento', ascending=True).head(quantidade_top_tweets)

    lista_links_bottom = '-------------\n{} tuítes (sem RTs) com menos engajamento\n\n'.format(quantidade_top_tweets)

    loop_bottom = 0

    for index, tweet in top_tweets_engajamento.iterrows():
        loop_bottom += 1

        lista_links_bottom = lista_links_bottom + '{}° - {} às {}'.format(str(loop_bottom),
                                                                          tweet['Data'], tweet['Hora'])

        lista_links_bottom = lista_links_bottom + '\nhttps://{}\n\n'.format(tweet['Url'])

    # Com mais engajamento no tema de interesse

    if analisando_tema_interesse:

        top_tweets_engajamento_interesse = dados_tema_interesse_srt.sort_values(by='Engajamento', ascending=False) \
            .head(quantidade_top_tweets_interesse)

        lista_links_top_interesse = '-------------\n{} tuítes (sem RTs) com mais engajamento no subtema de interesse\n\n'. \
            format(quantidade_top_tweets_interesse)

        loop_top_interesse = 0

        for index, tweet in top_tweets_engajamento_interesse.iterrows():
            loop_top_interesse += 1

            lista_links_top_interesse = lista_links_top_interesse + '{}° - {} às {}'.format(str(
                loop_top_interesse), tweet['Data'], tweet['Hora'])

            lista_links_top_interesse = lista_links_top_interesse + '\nhttps://{}\n\n'.format(tweet['Url'])

    # -----------------------------------------------------------------------------------------------------HASHTAGS

    # Gera lista com todas as hashtags
    hashtags = []
    contagem = []
    for index, tweet in dados_tema.iterrows():
        if tweet['Hashtags'] != '[]':
            for tag in tweet['Hashtags'].split(','):
                # Remove chars indesejados da string
                tag = tag.strip('[')
                tag = tag.strip(']')
                tag = tag.strip("'")

                # As vezes fica uma apóstrofe no início por alguma razão. Gambiarra pra remover isso:
                tag = tag.split('#')[-1]
                # Volta a #
                tag = '#' + tag

                # Incrementa o df
                hashtags.append(tag)
                contagem.append(1)

    # Cria df com colunas a partir das listas
    hashtags_df = pd.DataFrame(columns=['Hashtag', 'Contagem'])
    hashtags_df['Hashtag'] = hashtags
    hashtags_df['Contagem'] = contagem

    # transforma hashtags em suas versões minúsculas e sem acento
    hashtags_df['Hashtag'] = hashtags_df['Hashtag'].apply(lambda x: unidecode.unidecode(x.lower()))

    # Agrupa df por hashtag
    hashtags_df = hashtags_df.groupby(by='Hashtag', as_index=False).sum().sort_values(by='Contagem', ascending=False)

    # Salva csv do df
    salva_hashtags_contagem(hashtags_df, tema_n)

    # Top 5 hashtags ao longo do tempo

    # Lista com hashtags que mais aparecem
    top_hashtags = hashtags_df['Hashtag'][0:10]

    for tag in top_hashtags:
        # Pega tweets com a hashtag
        dados_tema['Hashtags min'] = dados_tema['Hashtags'].apply(lambda x : unidecode.unidecode(x.lower()))
        possui_tag = dados_tema['Hashtags min'].str.contains(tag) == True

        # relatório com tweets importantes da hashtag no período
        # Com RTs
        tweets_importantes_tag = dados_tema[possui_tag].sort_values(by='Engajamento', ascending=False)
        tweets_importantes_tag = tweets_importantes_tag.head(20)
        rel_tag = 'Principais tweets com {}\n\n'.format(tag)
        conta = 0
        for index, tweet in tweets_importantes_tag.iterrows():
            conta += 1
            rel_tag = rel_tag + '@{}\n' \
                                '{} em {} às {}\n' \
                                '{}\n' \
                                '{}\n' \
                                '{} Curtidas | {} Retweets\n' \
                                '\n\n'.format(
                conta,
                tweet['Usuário'],
                tweet['Data'],
                tweet['Hora'],
                tweet['Url'],
                tweet['Texto'],
                tweet['Curtidas'],
                tweet['Retweets']
            )
        # Sem RTs
        tweets_importantes_tag = dados_tema[possui_tag].sort_values(by='Engajamento', ascending=False)
        tweets_importantes_tag = tweets_importantes_tag[tweets_importantes_tag['Retwitado invert']]
        tweets_importantes_tag = tweets_importantes_tag.head(20)
        rel_tag = rel_tag + '\n\nTweets originais\n\n'.format(tag)
        conta = 0
        for index, tweet in tweets_importantes_tag.iterrows():
            conta += 1
            rel_tag = rel_tag + '@{}\n' \
                                '{} em {} às {}\n' \
                                '{}\n' \
                                '{}\n' \
                                '{} Curtidas | {} Retweets\n' \
                                '\n\n'.format(
                conta,
                tweet['Usuário'],
                tweet['Data'],
                tweet['Hora'],
                tweet['Url'],
                tweet['Texto'],
                tweet['Curtidas'],
                tweet['Retweets']
            )

        arquivo_rel_tag = open('resultados/análises 2/{}/hashtags/{}.txt'.format(tema_n, tag), 'w', encoding='utf-8')
        arquivo_rel_tag.write(rel_tag)
        arquivo_rel_tag.close()

        # Cria df com as quantidades por dia
        tag_por_dia = pd.DataFrame(columns=['Data', 'Frequência'])
        tag_por_dia['Data'] = dados_tema['DataPyt']
        tag_por_dia['Frequência'] = possui_tag
        tag_por_dia = tag_por_dia.groupby(by='Data', as_index=False).sum()

        # Acrescenta no df por dia a versão em int (sem isso tava indo str)
        por_dia[tag] = tag_por_dia['Frequência'].apply(lambda x: int(x))
        por_dia['n_' + tag] = por_dia[tag].apply(lambda x: (x - por_dia[tag].min()) / (por_dia[tag].max() - por_dia[tag].min()))

    # --------------------------------------------------------------------------------------------------------MENÇÕES

    # Gera lista com todas as mentions
    mentions = []
    contagem = []
    for index, tweet in dados_tema.iterrows():
        if tweet['Menções'] != '[]':
            for mention in tweet['Menções'].split(','):
                # Remove chars indesejados da string
                mention = mention.strip('[')
                mention = mention.strip(']')
                mention = mention.strip("'")

                # As vezes fica uma apóstrofe no início por alguma razão. Gambiarra pra remover isso:
                mention = mention.split('@')[-1]
                # Volta a #
                mention = '@' + mention

                # Incrementa o df
                mentions.append(mention)
                contagem.append(1)

    # Cria df com colunas a partir das listas
    mentions_df = pd.DataFrame(columns=['Usuário Mencionado', 'Contagem'])
    mentions_df['Usuário Mencionado'] = mentions
    mentions_df['Contagem'] = contagem

    # Agrupa df por usuário mencionado
    mentions_df = mentions_df.groupby(by='Usuário Mencionado', as_index=False).sum().sort_values(by='Contagem',
                                                                                                 ascending=False)
    # Exclui menções ao próprio usuário
    mention_a_outros = mentions_df['Usuário Mencionado'] != tema.strip('\n')
    mentions_df = mentions_df[mention_a_outros]

    # Salva csv do df
    salva_mentions_contagem(mentions_df, tema_n)

    # Lista com 5 menções que mais aparecem
    top_5_mentions = mentions_df['Usuário Mencionado'][0:10]

    for mention in top_5_mentions:
        # Pega tweets com a menções
        possui_mention = dados_tema['Menções'].str.contains(mention) == True

        # relatório com tweets importantes da mention no período
        # Com RTs
        tweets_importantes_mention = dados_tema[possui_mention].sort_values(by='Engajamento', ascending=False)
        tweets_importantes_mention = tweets_importantes_mention.head(20)
        rel_mention = 'Principais tweets que mencionam {}\n\n'.format(mention)
        conta = 0
        for index, tweet in tweets_importantes_mention.iterrows():
            conta += 1
            rel_mention = rel_mention + '{}\n' \
                                '{} em {} às {}\n' \
                                '{}\n' \
                                '{}\n' \
                                '{} Curtidas | {} Retweets\n' \
                                '\n\n'.format(
                conta,
                tweet['Usuário'],
                tweet['Data'],
                tweet['Hora'],
                tweet['Url'],
                tweet['Texto'],
                tweet['Curtidas'],
                tweet['Retweets']
            )
        # Sem RTs
        tweets_importantes_mention = dados_tema[possui_mention].sort_values(by='Engajamento', ascending=False)
        tweets_importantes_mention = tweets_importantes_mention[tweets_importantes_mention['Retwitado invert']]
        tweets_importantes_mention = tweets_importantes_mention.head(20)
        rel_mention = rel_mention + '\n\nTweets originais\n\n'.format(mention)
        conta = 0
        for index, tweet in tweets_importantes_mention.iterrows():
            conta += 1
            rel_mention = rel_mention + '{}\n' \
                                '{} em {} às {}\n' \
                                '{}\n' \
                                '{}\n' \
                                '{} Curtidas | {} Retweets\n' \
                                '\n\n'.format(
                conta,
                tweet['Usuário'],
                tweet['Data'],
                tweet['Hora'],
                tweet['Url'],
                tweet['Texto'],
                tweet['Curtidas'],
                tweet['Retweets']
            )

        arquivo_rel_mention = open('resultados/análises 2/{}/mentions/{}.txt'.format(tema_n, mention), 'w', encoding='utf-8')
        arquivo_rel_mention.write(rel_mention)
        arquivo_rel_mention.close()

        # Cria df com as quantidades por dia
        mention_por_dia = pd.DataFrame(columns=['Data', 'Frequência'])
        mention_por_dia['Data'] = dados_tema['DataPyt']
        mention_por_dia['Frequência'] = possui_mention
        mention_por_dia = mention_por_dia.groupby(by='Data', as_index=False).sum()

        # Acrescenta no df por dia
        por_dia['Menções a {}'.format(mention)] = mention_por_dia['Frequência'].apply(lambda x: int(x))
        por_dia['n_Menções a ' + mention] = por_dia['Menções a {}'.format(mention)]\
            .apply(lambda x: (x - por_dia['Menções a {}'.format(mention)].min()) / (por_dia['Menções a {}'.format(mention)].max() - por_dia['Menções a {}'.format(mention)].min()))

    # ---------------------------------------------------------------------------------------------------------REPLYS

    # Gera lista com todas replys
    replys = []
    contagem = []
    for index, tweet in dados_tema.iterrows():
        # Incrementa as listas
        replys.append(tweet['Reply'])
        contagem.append(1)

    # Cria df com colunas a partir das listas
    replys_df = pd.DataFrame(columns=['Resposta a', 'Contagem'])
    replys_df['Resposta a'] = replys
    replys_df['Contagem'] = contagem
    replys_df.dropna(inplace=True)

    # Agrupa df por replys
    replys_df = replys_df.groupby(by='Resposta a', as_index=False).sum().sort_values(by='Contagem', ascending=False)

    # Salva csv do df
    salva_replys_contagem(replys_df, tema_n)

    # Lista com 5 replys que mais aparecem
    top_replys = replys_df['Resposta a'][0:10]

    for reply in top_replys:
        # Pega tweets com a replys
        possui_reply = dados_tema['Reply'].str.contains(reply) == True

        # relatório com tweets importantes da hashtag no período
        # Com RTs
        tweets_importantes_reply = dados_tema[possui_reply].sort_values(by='Engajamento', ascending=False)
        tweets_importantes_reply = tweets_importantes_reply.head(20)
        rel_reply = 'Principais tweets que respondem a {}\n\n'.format(reply)
        conta = 0
        for index, tweet in tweets_importantes_reply.iterrows():
            conta += 1
            rel_reply = rel_reply + '@{}\n' \
                                '{} em {} às {}\n' \
                                '{}\n' \
                                '{}\n' \
                                '{} Curtidas | {} Retweets\n' \
                                '\n\n'.format(
                conta,
                tweet['Usuário'],
                tweet['Data'],
                tweet['Hora'],
                tweet['Url'],
                tweet['Texto'],
                tweet['Curtidas'],
                tweet['Retweets']
            )

        # Sem RTs
        tweets_importantes_reply = dados_tema[possui_reply].sort_values(by='Engajamento', ascending=False)
        tweets_importantes_reply = tweets_importantes_reply[tweets_importantes_reply['Retwitado invert']]
        tweets_importantes_reply = tweets_importantes_reply.head(20)
        rel_reply = rel_reply + '\n\nTweets originais\n\n'.format(reply)
        conta = 0
        for index, tweet in tweets_importantes_reply.iterrows():
            conta += 1
            rel_reply = rel_reply + '@{}\n' \
                                '{} em {} às {}\n' \
                                '{}\n' \
                                '{}\n' \
                                '{} Curtidas | {} Retweets\n' \
                                '\n\n'.format(
                conta,
                tweet['Usuário'],
                tweet['Data'],
                tweet['Hora'],
                tweet['Url'],
                tweet['Texto'],
                tweet['Curtidas'],
                tweet['Retweets']
            )

        arquivo_rel_reply = open('resultados/análises 2/{}/replys/{}.txt'.format(tema_n, reply), 'w', encoding='utf-8')
        arquivo_rel_reply.write(rel_reply)
        arquivo_rel_reply.close()

        # Cria df com as quantidades por dia
        reply_por_dia = pd.DataFrame(columns=['Data', 'Frequência'])
        reply_por_dia['Data'] = dados_tema['DataPyt']
        reply_por_dia['Frequência'] = possui_reply
        reply_por_dia = reply_por_dia.groupby(by='Data', as_index=False).sum()

        # Acrescenta no df por dia
        por_dia['Respostas a @{}'.format(reply)] = reply_por_dia['Frequência'].apply(lambda x: int(x))
        por_dia['n_Respostas a @' + reply] = por_dia['Respostas a @{}'.format(reply)]\
            .apply(lambda x: (x - por_dia['Respostas a @{}'.format(reply)].min()) / (por_dia['Respostas a @{}'.format(reply)].max() - por_dia['Respostas a @{}'.format(reply)].min()))

    # -----------------------------------------------------------------------------------------------------------MÍDIA

    # Gera lista com todos tipos de mídia
    media = []
    contagem = []
    for index, tweet in dados_tema.iterrows():
        if tweet['Mídia'] != '[]':
            # Incrementa as listas
            media.append(tweet['Mídia'].strip('[').strip(']').strip("'"))
            contagem.append(1)

    # Cria df com colunas a partir das listas
    media_df = pd.DataFrame(columns=['Tipo de Mídia', 'Contagem'])
    media_df['Tipo de Mídia'] = media
    media_df['Contagem'] = contagem
    media_df.dropna(inplace=True)

    # Agrupa df por tipo de mídia
    media_df = media_df.groupby(by='Tipo de Mídia', as_index=False).sum().sort_values(by='Contagem', ascending=False)

    # Salva csv do df
    salva_media_contagem(media_df, tema_n)

    # Lista com 5 tipos de mídia que mais aparecem
    top_5_media = media_df['Tipo de Mídia'][0:5]

    total_imagens = 0

    for media in top_5_media:
        # Pega tweets com o tipo de mídia
        possui_media = dados_tema['Mídia'].str.contains(media) == True

        # Cria df com as quantidades por dia
        media_por_dia = pd.DataFrame(columns=['Data', 'Frequência'])
        media_por_dia['Data'] = dados_tema['DataPyt']
        media_por_dia['Frequência'] = possui_media
        media_por_dia = media_por_dia.groupby(by='Data', as_index=False).sum()

        # Acrescenta no df por dia
        por_dia['Mídia: {}'.format(media)] = media_por_dia['Frequência'].apply(lambda x: int(x))

        if media == 'photo':
            total_imagens = media_por_dia['Frequência'].apply(lambda x: int(x))

    # -----------------------------------------------------------------------------------------------------TOP 100

    # Exporta dados dos 100 perfis com maior grau e relatórios dos tweets para avaliar o posicionamento de cada um
    top100 = dados_tema.groupby(by='Usuário', as_index=False).mean().sort_values('Degree', ascending=False).head(100)

    top100_perfil = []
    top100_pubs = []
    top100_engajamento = []
    top100_engajamento_medio = []
    top100_degree = []
    top100_eigen = []

    conta_perfil_top = 0
    for index, perfil in top100.iterrows():
        conta_perfil_top += 1
        dados_perfil = dados_tema[dados_tema['Usuário'] == perfil['Usuário']]
        dados_perfil = dados_perfil.sort_values(by='Curtidas', ascending=False).head(20)

        top100_perfil.append(perfil['Usuário'])
        top100_pubs.append(dados_perfil.shape[0])
        top100_engajamento.append(dados_perfil['Engajamento'].sum())
        top100_engajamento_medio.append(dados_perfil['Engajamento'].sum() / dados_perfil.shape[0])
        top100_degree.append(perfil['Degree'])
        top100_eigen.append(perfil['Eigenvector'])

        rel_top = 'Top 20 tweets de {} por curtidas\n\n'.format(perfil['Usuário'])
        conta_top = 0
        for index, tweet in dados_perfil.iterrows():
            conta_top += 1
            rel_top = rel_top + '@{}\n' \
                                    '{} em {} às {}\n' \
                                    '{}\n' \
                                    '{}\n' \
                                    '{} Curtidas | {} Retweets\n' \
                                    '\n\n'.format(
                conta_top,
                tweet['Usuário'],
                tweet['Data'],
                tweet['Hora'],
                tweet['Url'],
                tweet['Texto'],
                tweet['Curtidas'],
                tweet['Retweets']
            )

        arquivo_rel_top = open('resultados/análises 2/{}/top100/{}_{}.txt'.format(tema_n, conta_perfil_top, perfil['Usuário']), 'w', encoding='utf-8')
        arquivo_rel_top.write(rel_top)
        arquivo_rel_top.close()

    top100_export = pd.DataFrame()
    top100_export['Usuário'] = top100_perfil
    top100_export['Publicações'] = top100_pubs
    top100_export['Engajamento'] = top100_engajamento
    top100_export['Engajamento médio'] = top100_engajamento_medio
    top100_export['Degree'] = top100_degree
    top100_export['Eigenvector'] = top100_eigen
    top100_export.to_csv('resultados/análises 2/{}/top100/Top 100 perfis.csv'.format(tema_n), index=False)

    # -----------------------------------------------------------------------------------------------------POR DIA 2

    # Engajamento geral por dia
    por_dia_engajamento = dados_tema.sort_values('DataPyt').filter(items=['DataPyt', 'Engajamento']) \
        .groupby('DataPyt', as_index=False).sum()
    por_dia['Engajamento'] = por_dia_engajamento['Engajamento']

    # RTs geral por dia
    por_dia_retweets = dados_tema.sort_values('DataPyt').filter(items=['DataPyt', 'Retweets']) \
        .groupby('DataPyt', as_index=False).sum()
    por_dia['Retweets'] = por_dia_retweets['Retweets']

    # Curtidas geral por dia
    por_dia_curtidas = dados_tema.sort_values('DataPyt').filter(items=['DataPyt', 'Curtidas']) \
        .groupby('DataPyt', as_index=False).sum()
    por_dia['Curtidas'] = por_dia_curtidas['Curtidas']

    # Arrumando a data
    por_dia_data_arrumada = por_dia['Data'].apply(lambda linha_d:
                                                  datetime.strptime(linha_d, '%Y-%m-%d').strftime('%d/%m/%Y'))
    por_dia['Data_'] = por_dia_data_arrumada

    # Salva df
    salva_tweets_por_dia(por_dia, tema_n)
    por_dia.to_csv('resultados/análises 2/por dia/{}_por dia.csv'.format(tema), index=False)

    # ------------------------------------------------------------------------------------------------------COMPARATIVO

    # Acrescenta valores às listas que serão as colunas do df comparativo
    comp_id.append(tema_n)
    comp_tema.append(tema)
    comp_perfil.append(tema.strip('\n'))
    comp_tweets.append(dados_tema.shape[0])
    comp_tweets_srt.append(dados_tema_srt.shape[0])
    comp_tweets_dia_media_srt.append(tweets_dia_media)
    comp_rts_percentual.append(round(taxa_rts, 2))
    comp_replys_percentual.append(round(taxa_replys, 2))
    comp_engajamento_medio.append(round(engajamento_medio, 2))
    comp_engajamento_desvpad.append(round(desvio_padrao_engajamento, 2))
    comp_truncados_tx.append(round(taxa_de_truncados, 2))
    comp_truncados_tx_srt.append(round(taxa_de_truncados_srt, 2))
    comp_top_hashtags.append(hashtags_df['Hashtag'].head(5))
    comp_top_mentions.append(mentions_df['Usuário Mencionado'].head(5))
    comp_top_replys.append(replys_df['Resposta a'].head(5))
    comp_imagens.append(total_imagens)

    if analisando_tema_interesse:
        comp_tweets_tema.append(dados_tema_interesse.shape[0])
        comp_engajamento_tema.append(round(engajamento_medio_interesse, 2))
        comp_engajamento_percentual_do_geral.append(round(percentual_engajamento_medio, 2))
        comp_engajamento_tema_desvpad.append(round(desvio_padrao_engajamento_interesse, 2))

    # ------------------------------------------------------------------------------------FIM DO LOOP | RESUMO | ÍNDICE

    # Anota infos da análise no arquivo de links
    arquivo_links_interesse = abre_arquivo_links_interesse(tema_n)

    lista_links = '-------------\nTodos os tuítes sobre o subtema de interesse\n\n' + lista_links

    lista_links = '----------- LINKS ------------\n\n' + lista_links_top + lista_links_bottom + \
                  lista_links_top_interesse + lista_links

    arquivo_links_interesse.write(lista_links)

    tweets_perfil_media = round(dados_tema.shape[0] / dados_tema['Usuário'].nunique(), 2)
    tweets_perfil_media_srt = round(dados_tema_srt.shape[0] / dados_tema_srt['Usuário'].nunique(), 2)

    # Edita .txt com índice de temas pelos números
    indice_temas.write('Tema {} - {}\n\n'.format(tema_n, tema.strip('\n')))

    # Escreve relatório
    arquivo_relatorio = abre_relatorio(tema_n)

    arquivo_relatorio.write('Tema {} - {} - \n\n{} tuítes ({} com RTs) entre {} e {}\n'
                            .format(tema_n, tema.strip('\n'), dados_tema_srt.shape[0],
                                    dados_tema.shape[0], data_inicio, data_fim))

    arquivo_relatorio.write('{} perfis\n'.format(dados_tema['Usuário'].nunique()))

    arquivo_relatorio.write('{}% de RTs\n'.format(round(taxa_rts, 2)))
    arquivo_relatorio.write('{}% de replys\n'.format(round(taxa_replys, 2)))

    arquivo_relatorio.write('\n{} tuítes por dia em média (sem RTs)\n'.format(tweets_dia_media))

    arquivo_relatorio.write('\n{} tuítes por perfil em média\n'.format(tweets_perfil_media))

    arquivo_relatorio.write('\n{} tuítes originais por perfil em média\n'.format(tweets_perfil_media_srt))

    arquivo_relatorio.write('\nEngajamento médio em tuítes (Sem RTs): {}\nDesvio padrão: {}\n'
                            .format(round(engajamento_medio, 2), round(desvio_padrao_engajamento, 2)))

    arquivo_relatorio.write('\nTuítes truncados foram:\n{}% do total\n{}% dos tuítes originais\n'
                            .format(round(taxa_de_truncados, 2), round(taxa_de_truncados_srt, 2)))

    arquivo_relatorio.write('\nHashtags mais utilizadas\n')
    for index, tag in hashtags_df.head(5).iterrows():
        arquivo_relatorio.write('{} - {} ocorrências\n'.format(tag['Hashtag'], tag['Contagem']))

    arquivo_relatorio.write('\nPerfis mais mencionados\n')
    for index, mention in mentions_df.head(5).iterrows():
        arquivo_relatorio.write('{} - {} ocorrências\n'.format(mention['Usuário Mencionado'], mention['Contagem']))

    arquivo_relatorio.write('\nPerfis mais respondidos\n')
    for index, reply in replys_df.head(5).iterrows():
        arquivo_relatorio.write('{} - {} ocorrências\n'.format(reply['Resposta a'], reply['Contagem']))

    arquivo_relatorio.write('\nTipos de mídia mais usados\n')
    for index, media in media_df.head(5).iterrows():
        arquivo_relatorio.write('{} - {} ocorrências\n'.format(media['Tipo de Mídia'], media['Contagem']))

    if analisando_tema_interesse:
        arquivo_relatorio.write('\n----------- SUBTEMA DE INTERESSE -----------\n')

        arquivo_relatorio.write('\nSubtema de interesse analisado: {}\nPalavras-chave do tema de interesse: '
                                '{}\nTweets com palavras-chave de interesse: {}\n'
                                .format(tema_de_interesse, palavras_de_interesse, dados_tema_interesse.shape[0]))

        arquivo_relatorio.write('\nEngajamento médio em tuítes (Sem RTs) sobre o tema de interesse: {}\n'
                                .format(round(engajamento_medio_interesse, 2)))
        arquivo_relatorio.write('{}% da média geral\nDesvio padrão: {}\n\n'
                                .format(round(percentual_engajamento_medio, 2),
                                        round(desvio_padrao_engajamento_interesse, 2)))

        arquivo_relatorio.write(lista_links)

    t_final_tema = datetime.now()
    print('\n********\n\nTema OK\n\n********\n')
    print('Tempo de análise: {}'.format(t_final_tema - t_inicial_tema))

# ---------------------------------------------------------------------------------------------------------COMPARATIVOS

# Cria data frame de comparação
comparativo = pd.DataFrame()

comparativo['ID'] = comp_id
comparativo['Tema'] = comp_tema
comparativo['Usuário'] = comp_perfil
comparativo['Total de tuítes'] = comp_tweets
comparativo['Tuítes (sem RTs)'] = comp_tweets_srt
comparativo['Tuítes por dia'] = comp_tweets_dia_media_srt
comparativo['Taxa de RTs'] = comp_rts_percentual
comparativo['Taxa de replys'] = comp_replys_percentual
comparativo['Engajamento médio'] = comp_engajamento_medio
comparativo['Engajamento DesvPad'] = comp_engajamento_desvpad
comparativo['Taxa de truncados (total)'] = comp_truncados_tx
comparativo['Taxa de truncados (sem RTs)'] = comp_truncados_tx_srt
comparativo['Top 5 hashtags'] = comp_top_hashtags
comparativo['Top 5 mentions'] = comp_top_mentions
comparativo['Top 5 replys'] = comp_top_replys
comparativo['Imagens'] = comp_imagens

if analisando_tema_interesse:
    comparativo[tema_de_interesse] = comp_tweets_tema
    comparativo['Engajamento médio em {}'.format(tema_de_interesse)] = comp_engajamento_tema
    comparativo['Engajamento ({}/geral)'.format(tema_de_interesse)] = comp_engajamento_percentual_do_geral
    comparativo['Engajamento em {} DesvPad'.format(tema_de_interesse)] = comp_engajamento_tema_desvpad

comparativo.to_csv('resultados/análises 2/Comparativo.csv', index=False)

# PROVAVELMENTE NÃO SE APLICA A ESSA ANÁLISE
# # Cria data frame de comparação só com perfis que falaram sobre o tema de interesse
# falou_sobre_tema = comparativo[tema_de_interesse] > 0
# comparativo_interesse = comparativo[falou_sobre_tema]
# print(comparativo_interesse.info())
#
# comparativo_interesse.to_csv('resultados/análises 2/ComparativoTemaDeInteresse.csv', index=False)

# Salva corpus unificados
if gerando_corpus_unificado:
    arquivo_corpus_unificado_interesse.write(corpus_unificado_interesse)
    arquivo_corpus_unificado_interesse_links.write(corpus_unificado_interesse_links)
    arquivo_corpus_unificado.write(corpus_unificado)
    arquivo_corpus_unificado_srt.write(corpus_unificado)

print('----INÍCIO---{}-------'.format(t_inicial))
print('-------FIM---{}-------'.format(datetime.now()))
