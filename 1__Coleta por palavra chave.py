
# **********************************************************************************************************************

# Olá!
#
# Esses códigos foram desenvolvidos para o meu trabalho de conclusão de curso do MBA em marketing digital pelo UniCEUB.
# Não sou desenvolvedor, o código não está otimizado e tem muitos problemas. Mas é funcional e atende ao propósito muito bem.
# Esse programa busca tuítes por palavra-chave em um dado intervalo de tempo,
# guarda os dados de todos em um data frame e exporta esses dados para um csv

# **********************************************************************************************************************


# coding=utf-8

from TwitterSearch import *
import pandas as pd
import datetime
from datetime import timedelta
import time
import traceback

# IMPORTANTE! *****************************************************************************
# Define se é um teste ou não. Isso muda quais arquivos o programa lê e para quais exporta

testando = False

# Define se está agregando aos DFs que já existem ou se cria novos

acumulando = True

# ********************************************************************************************

# Timer para que o programa comece a rodar aproximadamente à meia noite. Isso evita problemas na contagem do tempo
horas_para_inicio = 0
minutos_para_inicio = 10
esperar_para_inicio = 3600*horas_para_inicio + 60*minutos_para_inicio
tempo = 0
while tempo <= esperar_para_inicio:
    print('faltam {} segundos para começar.'.format(esperar_para_inicio-tempo))
    time.sleep(1)
    tempo += 1

# ----------------------------------------------------------------------------------------------------------------


# Construtor do TwitterSearch com chaves e tokens de acesso ao twitter
acesso_api = open('AcessoTwitterAPI.txt')
acesso_lista = []
for linha in acesso_api:
    cod = linha.strip('\n')
    acesso_lista.append(cod)

key, secret, token, token_secret = acesso_lista

ts = TwitterSearch(consumer_key=key, consumer_secret=secret, access_token=token, access_token_secret=token_secret)

# Cria data frame pra armazenar resultados e metadados.
# O processo é cumulativo então os arquivos serão abertos e escritos novamente
variaveis = ['Url', 'Dia da semana', 'Data', 'Hora', 'Usuário', 'Curtidas', 'Retweets', 'Texto', 'Truncado',
             'Retwitado', 'Reply', 'Tema', 'Seguidores', 'Localização', 'Hashtags', 'Menções', 'Links', 'Mídia',
             'Mídia Url', 'Conta Verificada', 'UrlTema']
meta_variaveis = ['status', 'dia', 'hora', 'tweets desde', 'tweets até', 'tema', 'palavras-chave', 'tweets coletados',
                  'variáveis coletadas', 'Erro']

# Lê DFs já catalogados para acrescentar nova coleta
if acumulando:
    if testando:
        dados_velho = pd.DataFrame(pd.read_csv('resultados/teste/Base_de_Tweets_FINAL_teste.csv'))
    else:
        dados_velho = pd.DataFrame(pd.read_csv('resultados/real/Base_de_Tweets_FINAL.csv'))
else:
    dados_velho = pd.DataFrame()

dados = pd.DataFrame()

if testando:
    metadados = pd.DataFrame(pd.read_csv('resultados/teste/Log_Metadados_teste.csv'))
else:
    metadados = pd.DataFrame(pd.read_csv('resultados/real/Log_Metadados.csv'))

# Cria nomes de algumas variáveis
data_inicio = ''
data_fim =''
tweets_vistos = 0


# ------------------------------------------ Funções  ----------------------------------------------------------


def cria_lista_de_palavras_chave(l_testando):
    # lê arquivo com palavras-chave .txt e cria lista de palavras chave
    if l_testando:
        arquivo_de_palavras_chave = open('Tema,TodasPalavrasChave_teste.txt')
    else:
        arquivo_de_palavras_chave = open('Tema,TodasPalavrasChave.txt')

    lista = []

    for linha in arquivo_de_palavras_chave:
        sublista = linha.strip('\n').split(sep=',')
        lista.append(sublista)

    arquivo_de_palavras_chave.close()

    return lista


def espera_time_out(vistos, total, l_testando):
    # Checa se já viu tudo na query se sim aguarda 60s pra repetir
    if vistos == total:
        if l_testando:
            periodo = 1
            tempo_de_espera = 1
        else:
            periodo = 1
            tempo_de_espera = 1

        for ciclo in range(0, int(tempo_de_espera/periodo)):
            print('faltam {}s para recomeçar'.format(tempo_de_espera - periodo*ciclo))
            time.sleep(periodo)


def loga_metadados(etapa, mtdados, tema, termos, erro, l_testando):

    if etapa == 'início':
        # Loga metadados da coleta em csv externo
        erro = '-'
        metadados_inicio = pd.DataFrame([['coleta iniciada', datetime.date.today(), datetime.datetime.now().time(),
                                          data_inicio, data_fim, tema, termos, ' ', variaveis, erro]],
                                        columns=meta_variaveis)
        mtdados = pd.concat([mtdados, metadados_inicio], sort=True)

        if l_testando:
            mtdados.to_csv('resultados/teste/Log_Metadados_teste.csv', index=False)
        else:
            mtdados.to_csv('resultados/real/Log_Metadados.csv', index=False)

    if etapa == 'fim':
        erro = '-'
        # loga fim do processo no csv de metadados
        metadados_fim = pd.DataFrame([
            ['coleta encerrada com sucesso', datetime.date.today(), datetime.datetime.now().time(),
             data_inicio, data_fim, tema, termos, tweets_vistos_dia, variaveis, erro]], columns=meta_variaveis)
        mtdados = pd.concat([mtdados, metadados_fim])

        if l_testando:
            mtdados.to_csv('resultados/teste/Log_Metadados_teste.csv', index=False)
        else:
            mtdados.to_csv('resultados/real/Log_Metadados.csv', index=False)

    if etapa == 'erro':
        metadados_erro = pd.DataFrame([['coleta encerrada com erro', datetime.date.today(),
                                        datetime.datetime.now().time(), data_inicio, data_fim, tema, termos,
                                        tweets_vistos, variaveis, erro]], columns=meta_variaveis)
        mtdados = pd.concat([mtdados, metadados_erro])

        if l_testando:
            mtdados.to_csv('resultados/teste/Log_Metadados_teste.csv', index=False)
        else:
            mtdados.to_csv('resultados/real/Log_Metadados.csv', index=False)

    if etapa == 'limpeza':
        metadados_limpeza = pd.DataFrame([['limpeza de duplicatas', datetime.date.today(),
                                        datetime.datetime.now().time(), data_inicio, data_fim, tema, termos,
                                        duplicatas, variaveis, erro]], columns=meta_variaveis)
        mtdados = pd.concat([mtdados, metadados_limpeza])

        if l_testando:
            mtdados.to_csv('resultados/teste/Log_Metadados_teste.csv', index=False)
        else:
            mtdados.to_csv('resultados/real/Log_Metadados.csv', index=False)

    return mtdados


def guarda_dados(df, vistos, tema, termos, mtdados, l_testando):
    # Guardar dados dos tweets

    vistos_dia = 0

    loop = 0
    for tweet in ts.search_tweets_iterable(tso):

        try:
            # Infos da query
            queries, tweets_total = ts.get_statistics()

            # Registra progresso
            vistos += 1
            vistos_dia += 1
            if l_testando:
                print(
                    'TCC -- COLETA - {} - Dia: {} | {} -- QUERY: {} -- Tweets: {} de {} --'.format(tema, data_inicio,
                                                                                                          loop,
                                                                                                          queries,
                                                                                                          vistos,
                                                                                                          tweets_total))
            else:
                print(
                    'TCC -- COLETA - {} - Dia: {} | {} -- QUERY: {} -- Tweets: {} de {} --'.format(tema, data_inicio,
                                                                                                          loop,
                                                                                                          queries,
                                                                                                          vistos,
                                                                                                          tweets_total))
            loop += 1

            # Converte horário pra str_time
            tempo_original = tweet['created_at']
            tempo_str = time.strptime(tempo_original, '%a %b %d %H:%M:%S %z %Y')

            # Converte para segundos, subtrai 3h (brasil = gmt-3), volta para str_time
            fuso = -3600*3
            tempo_brasil = time.localtime(time.mktime(tempo_str) + fuso)

            # Armazena temporariamente pra montar o df

            # Tempo
            t_data = str(tempo_brasil[2]) + '/' + str(tempo_brasil[1]) + '/' + str(tempo_brasil[0])
            t_hora = str(tempo_brasil[3]) + ':' + str(tempo_brasil[4]) + ':' + str(tempo_brasil[5])
            t_dia = tempo_brasil[6]

            # Dados do usuário
            t_usuario = tweet['user']['screen_name']
            t_seguidores = tweet['user']['followers_count']
            t_localizacao = tweet['user']['location']
            t_verificado = tweet['user']['verified']

            # Engajamento
            t_curtidas = int(tweet['favorite_count'])
            t_retweets = int(tweet['retweet_count'])
            t_retwitado = tweet['retweeted']
            t_reply = tweet['in_reply_to_screen_name']

            # Links
            todos_links = tweet['entities']['urls']
            t_links = []
            for link in todos_links:
                t_links.append(link['url'])

            # Hashtags
            t_hashtags = []
            todas_hashtags = tweet['entities']['hashtags']
            for hashtag in todas_hashtags:
                t_hashtags.append('#' + hashtag['text'])

            # Menções
            t_mencoes = []
            todas_mencoes = tweet['entities']['user_mentions']
            for mencao in todas_mencoes:
                t_mencoes.append('@' + mencao['screen_name'])

            # Mídia
            t_midia = []
            t_midia_links = []
            if 'media' in tweet['entities']:
                todas_midias = tweet['entities']['media']
                for midia in todas_midias:
                    t_midia.append(midia['type'])
                    t_midia_links.append(midia['media_url_https'])

            # Outras variáveis
            t_texto = tweet['text']
            t_url = 'twitter.com/{}/status/{}'.format(tweet['user']['screen_name'], tweet['id_str'])
            t_truncado = tweet['truncated']
            t_tema = tema
            t_url_tema = t_url + ',' + t_tema

            # armazena no data frame e exporta parcial
            t_dados = pd.DataFrame([[t_url, t_dia, t_data, t_hora, t_usuario, t_curtidas, t_retweets, t_texto,
                                     t_truncado, t_retwitado, t_reply, t_tema, t_seguidores, t_localizacao,
                                     t_hashtags, t_mencoes, t_links, t_midia, t_midia_links, t_verificado, t_url_tema]],
                                   columns=variaveis)
            df = pd.concat([df, t_dados], ignore_index=True)

            if l_testando:
                df.to_csv('resultados/teste/Base_de_Tweets_PARCIAL_teste.csv', index=False)
            else:
                df.to_csv('resultados/real/Base_de_Tweets_PARCIAL.csv', index=False)

            espera_time_out(vistos, tweets_total, l_testando)

        except Exception as e:

            print('Erro: {}'.format(e))
            print(traceback.format_exc())
            print(dados.info())

            if l_testando:
                dados.to_csv('resultados/teste/Base_de_Tweets_ERRO_teste.csv', index=False)
            else:
                dados.to_csv('resultados/real/Base_de_Tweets_ERRO.csv', index=False)

            mtdados = loga_metadados('erro', metadados, tema, termos, e, l_testando)

    return df, vistos, vistos_dia, mtdados

# ------------------------------ INÍCIO --------------------------------------------------------------------------


palavras_chave = cria_lista_de_palavras_chave(testando)

# As datas de início e fim da busca são estabelecidas com base na data em que o programa é rodado,
# com base nessas variáveis:
periodo_dias = 8
passo_do_loop = 1

for loop_dia in range(periodo_dias):

    # Definindo datas de início e fim
    inicio_dias_atras = 8 - loop_dia
    data_inicio = datetime.date.today() - timedelta(days=inicio_dias_atras)
    data_fim = data_inicio + timedelta(days=passo_do_loop)
    print('---------------------------------------------------------------------------')
    print('Período: de {} até {}.'.format(data_inicio, data_fim))
    print('---------------------------------------------------------------------------\n')

    # loga metadados iniciais
    metadados = loga_metadados('início', metadados, '-', '-', '-', testando)

    # Loop de busca por palavra chave
    for linha in palavras_chave:

        tema = linha[0]
        termos_de_busca = linha[1:]

        try:

            # Criando uma 'ordem de busca' e definindo palavra chave, idioma e período
            tso = TwitterSearchOrder()
            tso.set_keywords(termos_de_busca, or_operator=True)
            tso.set_since(data_inicio)
            tso.set_until(data_fim)
            tso.set_result_type('mixed')
            tso.set_language('pt')

            dados, tweets_vistos, tweets_vistos_dia, metadados = guarda_dados(dados, tweets_vistos, tema,
                                                                              termos_de_busca, metadados, testando)

            metadados = loga_metadados('fim', metadados, tema, termos_de_busca, '-', testando)

        except Exception as e:
            print('Erro: {}'.format(e))
            print(traceback.format_exc())
            print(dados.info())

            if testando:
                dados.to_csv('resultados/teste/Base_de_Tweets_ERRO_teste.csv', index=False)
            else:
                dados.to_csv('resultados/real/Base_de_Tweets_ERRO.csv', index=False)

            metadados = loga_metadados('erro', metadados, tema, termos_de_busca, e, testando)

    # Espera 15 min após cada dia. SERÁ QUE VALE A PENA?
    # time.sleep(15*60)

# Junta com o df velho
dados_novo = pd.concat([dados_velho, dados])

# deleta duplicatas por link que é uma identificação realmente única para cada tweet
duplicatas = dados_novo.shape[0] - dados_novo.drop_duplicates(subset='UrlTema').shape[0]
dados_novo.drop_duplicates(subset='UrlTema', keep='last', inplace=True)
print('-------------DUPLICATAS---------------\n')
print('Foram removidas {} linhas duplicadas'.format(duplicatas))
print('---------------------------------------\n')

loga_metadados('limpeza', metadados, '-', '-', '-', testando)

# salva dados coletados no csv
if testando:
    dados_novo.to_csv('resultados/teste/Base_de_Tweets_FINAL_teste.csv', index=False)
else:
    dados_novo.to_csv('resultados/real/Base_de_Tweets_FINAL.csv', index=False)

# termina o programa
print('-*- Fim -*-')
print(dados_novo.info())
print('-*- Fim -*-')