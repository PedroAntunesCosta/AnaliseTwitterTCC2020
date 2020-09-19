
# **********************************************************************************************************************

# Olá!
#
# Esses códigos foram desenvolvidos para o meu trabalho de conclusão de curso do MBA em marketing digital pelo UniCEUB.
# Não sou desenvolvedor, o código não está otimizado e tem muitos problemas. Mas é funcional e atende ao propósito muito bem.
# Esse programa busca tuítes por perfil em um dado intervalo de tempo,
# guarda os dados de todos em um data frame e exporta esses dados para um csv

# **********************************************************************************************************************

# coding=utf-8

from TwitterSearch import *
import pandas as pd
import datetime
from datetime import timedelta
from datetime import datetime
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
minutos_para_inicio = 0
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
             'Retwitado', 'Reply', 'Usuário buscado', 'Seguidores', 'Localização', 'Hashtags', 'Menções', 'Links',
             'Mídia', 'Mídia Url', 'Conta Verificada', 'UrlTema']
meta_variaveis = ['status', 'dia', 'hora', 'tweets desde', 'tweets até', 'Usuário buscado', 'tweets coletados',
                  'variáveis coletadas', 'Erro']

# Lê DFs já catalogados para acrescentar nova coleta
if acumulando:
    if testando:
        metadados = pd.DataFrame(pd.read_csv('resultados/teste/Log_Metadados_teste.csv'))
    else:
        metadados = pd.DataFrame(pd.read_csv('resultados/real/Log_Metadados.csv'))

    if testando:
        dados_velho = pd.DataFrame(pd.read_csv('resultados/teste/Base_de_Tweets_FINAL_teste.csv'))
    else:
        dados_velho = pd.DataFrame(pd.read_csv('resultados/real/Base_de_Tweets_FINAL.csv'))
else:
    metadados = pd.DataFrame()
    dados_velho = pd.DataFrame()

dados = pd.DataFrame()
dados_novo = pd.DataFrame()

# Cria nomes de algumas variáveis
data_inicio = ''
data_fim =''
tweets_vistos = 0


# ------------------------------------------ Funções  ----------------------------------------------------------


def cria_lista_de_usuarios(l_testando):
    # lê arquivo com palavras-chave .txt e cria lista de palavras chave
    if l_testando:
        arquivo_de_usuarios = open('BuscarUsuários_teste.txt')
    else:
        arquivo_de_usuarios = open('BuscarUsuários.txt')

    lista = []

    for linha in arquivo_de_usuarios:
        usuario = linha.strip('@')
        lista.append(usuario)

    arquivo_de_usuarios.close()

    return lista


def espera_time_out(vistos, total, l_testando):
    # Checa se já viu tudo na query se sim aguarda 60s pra repetir
    if vistos == total:
        if l_testando:
            periodo = 1
            tempo_de_espera = 1
        else:
            periodo = 10
            tempo_de_espera = 60

        for ciclo in range(0, int(tempo_de_espera/periodo)):
            print('faltam {}s para recomeçar'.format(tempo_de_espera - periodo*ciclo))
            time.sleep(periodo)


def loga_metadados(etapa, mtdados, usuario, erro, l_testando):

    if etapa == 'início':
        # Loga metadados da coleta em csv externo
        erro = '-'
        metadados_inicio = pd.DataFrame([['coleta iniciada', datetime.today(), datetime.now().time(),
                                          data_inicio, data_fim, usuario, ' ', variaveis, erro]],
                                        columns=meta_variaveis)
        mtdados = pd.concat([mtdados, metadados_inicio])

        if l_testando:
            mtdados.to_csv('resultados/teste/Log_Metadados_teste.csv', index=False)
        else:
            mtdados.to_csv('resultados/real/Log_Metadados.csv', index=False)

    if etapa == 'fim':
        erro = '-'
        # loga fim do processo no csv de metadados
        metadados_fim = pd.DataFrame([
            ['coleta encerrada com sucesso', datetime.today(), datetime.now().time(),
             data_inicio, data_fim, usuario, tweets_vistos_dia, variaveis, erro]], columns=meta_variaveis)
        mtdados = pd.concat([mtdados, metadados_fim])

        if l_testando:
            mtdados.to_csv('resultados/teste/Log_Metadados_teste.csv', index=False)
        else:
            mtdados.to_csv('resultados/real/Log_Metadados.csv', index=False)

    if etapa == 'erro':
        metadados_erro = pd.DataFrame([['coleta encerrada com erro', datetime.today(),
                                        datetime.now().time(), data_inicio, data_fim, usuario,
                                        tweets_vistos, variaveis, erro]], columns=meta_variaveis)
        mtdados = pd.concat([mtdados, metadados_erro])

        if l_testando:
            mtdados.to_csv('resultados/teste/Log_Metadados_teste.csv', index=False)
        else:
            mtdados.to_csv('resultados/real/Log_Metadados.csv', index=False)

    if etapa == 'limpeza':
        metadados_limpeza = pd.DataFrame([['limpeza de duplicatas', datetime.today(),
                                        datetime.now().time(), data_inicio, data_fim, usuario,
                                        duplicatas, variaveis, erro]], columns=meta_variaveis)
        mtdados = pd.concat([mtdados, metadados_limpeza])

        if l_testando:
            mtdados.to_csv('resultados/teste/Log_Metadados_teste.csv', index=False)
        else:
            mtdados.to_csv('resultados/real/Log_Metadados.csv', index=False)

    return mtdados


def guarda_dados(df, vistos, usuario, mtdados, l_testando):
    # Guardar dados dos tweets

    vistos_dia = 0

    loop = 0
    for tweet in ts.search_tweets_iterable(tuo):

        try:
            # Infos da query
            queries, tweets_total = ts.get_statistics()

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
            t_usuario = tweet['user']['screen_name'].strip('\n')
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
            t_url_usuario = t_url + ',' + usuario

            # armazena no data frame e exporta parcial
            t_dados = pd.DataFrame([[t_url, t_dia, t_data, t_hora, t_usuario, t_curtidas, t_retweets, t_texto,
                                     t_truncado, t_retwitado, t_reply, usuario, t_seguidores, t_localizacao,
                                     t_hashtags, t_mencoes, t_links, t_midia, t_midia_links, t_verificado, t_url_usuario]],
                                   columns=variaveis)
            df = pd.concat([df, t_dados], ignore_index=True)

            if l_testando:
                df.to_csv('resultados/teste/Base_de_Tweets_PARCIAL_teste.csv', index=False)
            else:
                df.to_csv('resultados/real/Base_de_Tweets_PARCIAL.csv', index=False)

            espera_time_out(vistos, tweets_total, l_testando)

            # Registra progresso
            vistos += 1
            vistos_dia += 1
            if l_testando:
                print(
                    'Tweets por usuário-- TESTE - {} - Dia: {} | {} -- QUERY: {} -- Tweets: {} de {} --'.format(usuario,
                                                                                                                t_data,
                                                                                                                loop,
                                                                                                                queries,
                                                                                                                vistos,
                                                                                                                tweets_total))
            else:
                print(
                    'Tweets por usuário-- COLETA - {} - Dia: {} | {} -- QUERY: {} -- Tweets: {} de {} --'.format(
                        usuario,
                        t_data,
                        loop,
                        queries,
                        vistos,
                        tweets_total))

            print('---{}'.format(df.shape[0]))

        except Exception as e:

            print('Erro: {}'.format(e))
            print(traceback.format_exc())
            print(dados.info())

            if l_testando:
                dados.to_csv('resultados/teste/Base_de_Tweets_ERRO_teste.csv', index=False)
            else:
                dados.to_csv('resultados/real/Base_de_Tweets_ERRO.csv', index=False)

            mtdados = loga_metadados('erro', metadados, usuario, e, l_testando)

    return df, vistos, vistos_dia, mtdados

# ------------------------------ INÍCIO --------------------------------------------------------------------------


t_inicial = datetime.now()

lista_usuarios = cria_lista_de_usuarios(testando)

# loga metadados iniciais
metadados = loga_metadados('início', metadados, '-', '-', testando)

# Loop de busca por palavra chave
for usuario in lista_usuarios:

    try:

        # Criando uma 'ordem de busca' e definindo palavra chave, idioma e período
        tuo = TwitterUserOrder(usuario)

        dados_novo, tweets_vistos, tweets_vistos_dia, metadados = guarda_dados(pd.DataFrame(), tweets_vistos, usuario, metadados, testando)

        metadados = loga_metadados('fim', metadados, usuario, '-', testando)

        dados = pd.concat([dados, dados_novo], ignore_index=True)
        print('Concluido usuário. Total: {}'.format(dados.shape[0]))

    except Exception as e:
        print('Erro: {}'.format(e))
        print(traceback.format_exc())
        print(dados.info())

        dados = pd.concat([dados, dados_novo], ignore_index=True)

        if testando:
            dados.to_csv('resultados/teste/Base_de_Tweets_ERRO_teste.csv', index=False)
        else:
            dados.to_csv('resultados/real/Base_de_Tweets_ERRO.csv', index=False)

        metadados = loga_metadados('erro', metadados, usuario, e, testando)



# deleta duplicatas por link que é uma identificação realmente única para cada tweet
dados = pd.concat([dados_velho, dados], ignore_index=True)
duplicatas = dados.shape[0] - dados.drop_duplicates(subset='UrlTema').shape[0]
dados.drop_duplicates(subset='UrlTema', keep='last', inplace=True)
print('Foram removidas {} linhas duplicadas'.format(duplicatas))
loga_metadados('limpeza', metadados, '-', '-', testando)

# salva dados coletados no csv
if testando:
    dados.to_csv('resultados/teste/Base_de_Tweets_FINAL_teste.csv', index=False)
else:
    dados.to_csv('resultados/real/Base_de_Tweets_FINAL.csv', index=False)

# termina o programa
print('-*- Fim -*-')
print(dados.info())
print('-*- Fim -*-')

print('-------INÍCIO---{}-------'.format(t_inicial))
print('-------FIM---{}-------'.format(datetime.now()))
