
# **********************************************************************************************************************

# Olá!
#
# Esses códigos foram desenvolvidos para o meu trabalho de conclusão de curso do MBA em marketing digital pelo UniCEUB.
# Não sou desenvolvedor, o código não está otimizado e tem muitos problemas. Mas é funcional e atende ao propósito muito bem.
# Esse programa cria um grafo a partir da base de tuítes e exporta diversos produtos de análise de redes, incluindo o grafo no formato do Gephi

# **********************************************************************************************************************

# coding=utf-8
import datetime
import numpy as np
import pandas as pd
import os
import shutil
import networkx as nx
import matplotlib.pyplot as plt
from community import community_louvain
import traceback
# ------------------------------------------------------------------------------------------------------------VARIÁVEIS

# Seleciona medida de centralidade a ser  utilizada para destacar principais perfis de cada cluster. Pode ser Degree,
# Eigenvector ou Betweeness.
medida_de_centralidade = 'Degree'

# Define porcentagem mínimo de perfis para uma comunidade ser levada em consideração
# testando fazer com mínimo relativo ao tamanho da rede
porcentagem_minima = 0.1

# Define número de perfis a serem mostrados no geral
top_mostrar = 10

# Define número de perfis a ser mostrado em cada comunidade
top_mostrar_2 = 10

# Define número de publicações de cada comunidade a serem mostradas
top_pub_mostrar = 10

# A resolução influencia no número de comunidades detectadas. Quanto maior a resolução mais comunidades serão detectadas
resolucao_inicial = 10

# Quantidade de comunidades desejada
qt_comunidades_desejada = 2

# Fazer método betweeness? Essa parte costuma demorar então se não for um objetivo específico, melhor não fazer
fazendo_betweeness = False

# Consolidar dados em data frame do pandas e depois csv? Essa fase está demorando muito. Vou tentar um caminho sem isso
consolidando_dados = True

# Fazer figura?
fazendo_figura = False

# --------------------------------------------------------------------------------------------------------------INSUMOS
# Leitura dos temas a serem analisados e criação de pastas
# *Temas devem estar listados no txt exatamente com a mesma grafia usada para documentar a busca
temas = []
arquivo_temas = open('TemasAnalisarRedes.txt')
for linha in arquivo_temas:
    temas.append(linha.strip('\n'))

print('------------------\nTEMAS\n------------------')
print(temas)

# Importação dos dados
dados = pd.DataFrame(pd.read_csv('dados/prev e prevsen_para redes.csv'))

dados['Engajamento'] = dados.apply(lambda linha: linha['Curtidas'] + linha['Retweets'], axis=1)

print('------------------\nREUSMO DA BASE DE DADOS\n------------------')
print(dados.info())

tempo_inicial = datetime.datetime.now()

# Relatório geral

arquivo_relatorio_geral = open('resultados/relatorio_redes_geral.txt', 'w', encoding='utf-8')
arquivo_relatorio_geral.write('RELATÓRIO GERAL DA ANÁLISE DE REDES - {}\n\n'
                              'Temas: {}\n'
                              'Resolução inicial: {}\n'
                              'Comunidades desejadas: {}\n'
                              'Porcentagem mínima de perfis na comunidade: {}%\n'
                              '--------------------------------------------------------------------\n\n'
                              .format(datetime.datetime.now(), temas, resolucao_inicial, qt_comunidades_desejada,
                                      porcentagem_minima*100))

# -------------------------------------------------------------------------------------------------COMEÇA LOOP POR TEMA
tema_n = 0

for tema in temas:

    tema_n += 1

    tempo_inicial_loop = datetime.datetime.now()

    print('------------------\nTema {} - {}\n------------------'.format(tema_n, tema))

    # Seleção dos dados por tema
    dados_tema = dados[dados['Tema'] == tema]

    # Se o tema não está na base de dados avisa e segue o programa
    if dados_tema.shape[0] == 0:
        print('*****Não há dados sobre o tema')
        continue

    print('*****Base de dados do tema:')
    print(dados_tema.shape[0])
    print(dados_tema.info())

    # Gerencia pastas
    pasta = 'resultados/{}_{}_Análise de Redes'.format(tema_n, tema)
    if os.path.isdir(pasta):
        shutil.rmtree(pasta)

    inicio = dados_tema['Data'].min()
    fim = dados_tema['Data'].max()

    os.makedirs(pasta)

# ---------------------------------------------------------------------------------------------------------------NODES

    nodes = pd.DataFrame()
    perfis = dados_tema['Usuário'].unique()

    # Variáveis dos nodes
    lista_perfis = []
    seguidores = []
    tweets = []
    curtidas = []
    retweets = []
    engajamento = []
    verificado =[]
    # lista de tuples das características dos perfis dos nodes -- ainda não tá sendo utilizado
    nodes_resto = []

    for perfil in perfis:
        # print('Perfil: {}'.format(perfil))
        dados_tema_perfil = dados_tema[dados_tema['Usuário'] == perfil]

        # Acrescenta variáveis às respectivas listas
        lista_perfis.append(perfil.strip(' '))
        seguidores.append(dados_tema_perfil['Seguidores'].iloc[0])
        tweets.append(dados_tema_perfil.shape[0])
        curtidas.append(sum(dados_tema_perfil['Curtidas']))
        retweets.append(sum(dados_tema_perfil['Retweets']))
        engajamento.append(sum(dados_tema_perfil['Engajamento']))
        verificado.append(dados_tema_perfil['Conta Verificada'].iloc[0])

    # Acrescenta todas as listas como colunas do data frame
    nodes['ID'] = lista_perfis
    nodes['Usuário'] = lista_perfis
    nodes['Seguidores'] = seguidores
    nodes['Publicações'] = tweets
    nodes['Curtidas'] = curtidas
    nodes['Retweets'] = retweets
    nodes['Engajamento'] = engajamento
    nodes['Verificado'] = verificado

    print('****____Nodes')
    print(nodes.shape[0])

    nodes.to_csv('{}/nodes_{}.csv'.format(pasta, tema))

# ----------------------------------------------------------------------------------------------------------------EDGES

    edges = pd.DataFrame()
    edges_export = pd.DataFrame()

    source = []
    target = []

    for index, tweet in dados_tema.iterrows():

        # Se houver mentions, iterar para cada mention acrescentanto source e target
        if tweet['Menções'] != '[]':

            dif = 0
            ordem = 0
            for mention in tweet['Menções'].split(','):
                ordem += 1
                # Remove chars indesejados da string
                mention = mention.strip('[')
                mention = mention.strip(']')
                mention = mention.strip("'")
                mention = mention.strip(' ')

                if ordem == 1:
                    mention = mention[1:]
                else:
                    mention = mention[2:]


                source.append(tweet['Usuário'].strip(' '))
                target.append(mention)

    # Monta df final com as colunas 'Source' e 'Target' das listas já feitas

    edges['Source'] = source
    edges['Target'] = target

    edges_export['Source'] = source
    edges_export['Target'] = target
    edges_export['Type'] = 'Undirected'
    edges_export['Weight'] = 1

    edges_export.to_csv('{}/edges_{}.csv'.format(pasta, tema))

    print('****____Edges')
    print(edges.shape[0])

    # geração de lista de tuples pra networkx
    edges_tuple = [tuple(x) for x in edges.to_records(index=False)]

# ------------------------------------------------------------------------------------------------- CONSTRUÇÃO DO GRAFO

    # cria grafo
    grafo = nx.Graph()

    # limpa grafo caso haja resíduo do último loop
    grafo.clear()

    # acrescenta nodes e edges
    grafo.add_nodes_from(lista_perfis)
    grafo.add_edges_from(edges_tuple)

    # acrescenta atributos aos grafos

    # cria dicts de atributos
    n_perfis = {}
    n_seguidores = {}
    n_tweets = {}
    n_curtidas = {}
    n_retweets = {}
    n_engajamento = {}
    n_verificado = {}

    # preenche os dicts
    contador = 0
    for perfil in grafo.nodes:
        # para perfis que publicaram
        if perfil in lista_perfis:
            n_perfis[perfil] = lista_perfis[contador]
            n_seguidores[perfil] = seguidores[contador]
            n_tweets[perfil] = tweets[contador]
            n_curtidas[perfil] = curtidas[contador]
            n_retweets[perfil] = retweets[contador]
            n_engajamento[perfil] = engajamento[contador]
            n_verificado[perfil] = bool(verificado[contador])
            contador += 1
        # para perfis que não publicaram
        else:
            n_perfis[perfil] = perfil
            n_seguidores[perfil] = 'Na'
            n_tweets[perfil] = 0
            n_curtidas[perfil] = 0
            n_retweets[perfil] = 0
            n_engajamento[perfil] = 0
            n_verificado[perfil] = 'Na'

    # atribui os dicts como atributos dos nodes
    nx.set_node_attributes(grafo, n_perfis, 'Perfil')
    nx.set_node_attributes(grafo, n_tweets, 'Publicações')
    nx.set_node_attributes(grafo, n_curtidas, 'Curtidas')
    nx.set_node_attributes(grafo, n_engajamento, 'Engajamento')
    nx.set_node_attributes(grafo, n_retweets, 'Retweets')
    nx.set_node_attributes(grafo, n_seguidores, 'Seguidores')
    nx.set_node_attributes(grafo, n_verificado, 'Verificado')

    print('\n\n------Grafo feito ----------------------------------')

    print(nx.info(grafo))

    # registra nodes isolados e exclui
    isolados = list(nx.isolates(grafo))
    arquivo_isolados = open('{}/nodes isolados_{}.txt'.format(pasta, tema), 'w', encoding='utf-8')
    arquivo_isolados.write('Foram removidos {} nodes isolados: \n\n'.format(len(isolados)))

    for node_isolado in isolados:
        arquivo_isolados.write('{}\n'.format(node_isolado))
    grafo.remove_nodes_from(isolados)
    print('-------------- {} nodes isolados removidos -------'.format(len(isolados)))

    # registra nodes em componentes isolados pequenos e exclui
    arquivo_componentes_pequenos = open('{}/componentes isolados pequenos_{}.txt'.format(pasta, tema), 'w',
                                        encoding='utf-8')
    componentes_pequenos_registrar = ''
    contador = 0
    conta_nodes_removidos = 0
    perfis_minimo = round(len(grafo)*porcentagem_minima)

    for componente in list(nx.connected_components(grafo)):

        if len(componente) < perfis_minimo:
            contador += 1
            componentes_pequenos_registrar = componentes_pequenos_registrar + '------------------------------------\n' \
                                                                              'Componente {}\n' \
                                                                              '{} nodes removidos:\n'\
                .format(contador, len(componente))

            for node_cp in componente:
                conta_nodes_removidos += 1
                grafo.remove_node(node_cp)
                componentes_pequenos_registrar = componentes_pequenos_registrar + '{}\n'.format(node_cp)

            componentes_pequenos_registrar = componentes_pequenos_registrar + '\n'

    arquivo_componentes_pequenos.write('Foram removidos {} nodes de {} componentes pequenos isolados:\n\n'
                                       .format(conta_nodes_removidos, contador) + componentes_pequenos_registrar)
    print('Foram removidos {} nodes de {} componentes pequenos isolados'.format(conta_nodes_removidos, contador))

    # Exporta grafo para o Gephi
    nx.write_gexf(grafo, '{}/{}_completo.gexf'.format(pasta, tema))

# ---------------------------------------------------------------------------------------------------- MÉTRICAS DA REDE

    print('\n-----Densidade: {}'.format(nx.density(grafo)))
    componentes = nx.connected_components(grafo)

    # centralidade
    # 1. degree
    degree_dict = dict(grafo.degree(grafo.nodes()))
    nx.set_node_attributes(grafo, degree_dict, 'Degree')
    print('---{}--Degree ok'.format(datetime.datetime.now()))

    # 2. eigenvector
    # Como eigen pode dar erros dependendo do método, tem que tentar maneiras diferentes
    eigen_metodo = ''
    try:
        eigenvector_dict = nx.eigenvector_centrality(grafo)
        nx.set_node_attributes(grafo, eigenvector_dict, 'Eigenvector')
        eigen_metodo = 'Default'
    except:

        try:
            eigenvector_dict = nx.eigenvector_centrality_numpy(grafo)
            nx.set_node_attributes(grafo, eigenvector_dict, 'Eigenvector')
            eigen_metodo = 'Numpy'

        except:
            eigen_metodo = 'Erro'
            eigenvector_dict = {}

            for perfil in grafo.nodes:
                eigenvector_dict[perfil] = 'Erro'

            nx.set_node_attributes(grafo, eigenvector_dict, 'Eigenvector')
    print('---{}--Eigen ok'.format(datetime.datetime.now()))

    # 3. betweeness
    if fazendo_betweeness:
        betweenness_dict = nx.betweenness_centrality(grafo)
        nx.set_node_attributes(grafo, betweenness_dict, 'Betweeness')
        print('---{}--Betweeness ok'.format(datetime.datetime.now()))

    # 4. modularidade (detecção de comunidades)

    # enquanto o número de comunidades maiores que o mínimo de perfis for diferente de 2 ele tenta de novo mudando a
    # resolução

    qt_comunidades = 0
    resolucao = resolucao_inicial
    contador = 0

    while qt_comunidades != qt_comunidades_desejada:
        contador += 1
        comunidades = community_louvain.best_partition(graph=grafo, resolution=resolucao)
        comu_df = pd.DataFrame.from_dict(comunidades, orient='index', columns=['Modularidade'])
        comu_df['Qt de perfis'] = 1
        print('\ninfo-------------------\n')
        print(comu_df.info())
        comu_df_agrupado = comu_df.groupby('Modularidade').sum().sort_values(by='Qt de perfis', ascending=False)
        qt_comunidades = (comu_df_agrupado['Qt de perfis'] > perfis_minimo).sum()
        print('tentativa----------------------------')
        print(contador)
        print('perfis mínimo-------------------------------')
        print(perfis_minimo)
        print('resolução--------------------------')
        print(resolucao)
        print('quantidade de comunidades-----------------------')
        print(qt_comunidades)

        if qt_comunidades > qt_comunidades_desejada:
            resolucao = resolucao*10

        if qt_comunidades < qt_comunidades_desejada:
            resolucao = resolucao/2

        if qt_comunidades < 1:
            resolucao = resolucao * 20

        if contador > 100:
            qt_comunidades_desejada = qt_comunidades_desejada - 2
            contador = 0
            resolucao = resolucao_inicial

    nx.set_node_attributes(grafo, comunidades, 'Modularidade')

    print('---{}--Modularidade ok'.format(datetime.datetime.now()))

    # Relatório geral
    arquivo_relatorio_geral.write('{} - {}\n\n'
                                  'Resolução: {}\n'
                                  'Mínimo de perfis: {}\n\n'
                                  'Comunidades:'
                                  .format(tema_n, tema, resolucao, perfis_minimo))
    for comu in comu_df_agrupado['Qt de perfis'].head(qt_comunidades_desejada):
        arquivo_relatorio_geral.write('- {} perfis - {}% do total'.format(comu, round(comu/len(grafo), 2)*100))

    # Exporta grafo para o Gephi
    nx.write_gexf(grafo, '{}/{}_Reduzido.gexf'.format(pasta, tema))

# ---------------------------------------------------------------------------------------------- CONSOLIDAÇÃO DOS DADOS

    # pegando atributos do grafo e passando pra data frame e depois csv
    dados_perfis_rede = pd.DataFrame()
    dados_tema['Degree'] = np.nan
    dados_tema['Eigenvector'] = np.nan
    dados_tema['Modularidade'] = np.nan
    if fazendo_betweeness:
        dados_tema['Betweeness'] = np.nan

    if consolidando_dados:

        # DOS PERFIS

        a_perfis = []
        a_tweets = []
        a_curtidas = []
        a_retweets = []
        a_engajamento = []
        a_seguidores = []
        a_verificado = []
        a_degree = []
        a_eigenvector = []
        a_modularidade = []
        if fazendo_betweeness:
            a_betweeness = []

        contador = 0
        progresso = 0
        for node in grafo.nodes():
            contador += 1
            progresso = round(contador/len(grafo)*100, 2)
            print('---{}--|{}|--dados do node para lista | progresso {} | node {}'
                  .format(datetime.datetime.now(), tema, progresso, node))
            a_perfis.append(nx.get_node_attributes(grafo, 'Perfil')[node])
            a_tweets.append(nx.get_node_attributes(grafo, 'Publicações')[node])
            a_curtidas.append(nx.get_node_attributes(grafo, 'Curtidas')[node])
            a_retweets.append(nx.get_node_attributes(grafo, 'Retweets')[node])
            a_engajamento.append(nx.get_node_attributes(grafo, 'Engajamento')[node])
            a_seguidores.append(nx.get_node_attributes(grafo, 'Seguidores')[node])
            a_verificado.append(nx.get_node_attributes(grafo, 'Verificado')[node])
            a_degree.append(nx.get_node_attributes(grafo, 'Degree')[node])
            a_eigenvector.append(nx.get_node_attributes(grafo, 'Eigenvector')[node])
            a_modularidade.append(nx.get_node_attributes(grafo, 'Modularidade')[node])
            if fazendo_betweeness:
                a_betweeness.append(nx.get_node_attributes(grafo, 'Betweeness')[node])

        print('=========================\n---{}--dados do node pra lista ok\n'.format(datetime.datetime.now()))

        dados_perfis_rede['Usuário'] = a_perfis
        dados_perfis_rede['Publicações'] = a_tweets
        dados_perfis_rede['Curtidas'] = a_curtidas
        dados_perfis_rede['Retweets'] = a_retweets
        dados_perfis_rede['Engajamento'] = a_engajamento
        dados_perfis_rede['Seguidores'] = a_seguidores
        dados_perfis_rede['Verificado'] = a_verificado
        dados_perfis_rede['Degree'] = a_degree
        dados_perfis_rede['Eigenvector'] = a_eigenvector
        dados_perfis_rede['Modularidade'] = a_modularidade
        if fazendo_betweeness:
            dados_perfis_rede['Betweeness'] = a_betweeness

        dados_perfis_rede.to_csv('{}/dados perfis rede_{}.csv'.format(pasta, tema), index=False)

        print('---{}--dados dos perfis para csv ok\n'.format(datetime.datetime.now()))

        # DOS TWEETS

        # passando métricas de rede para data frame com as publicações

        contador = 0
        conta_erro = 0
        progresso = 0
        for perfil in dados_tema['Usuário']:
            progresso = round((contador + 1) / dados_tema.shape[0] * 100, 2)
            print('******{}--|{}|--Passando dados de rede pro dataframe de rede | progresso {}'
                  .format(datetime.datetime.now(), tema, progresso))
            if perfil.strip(' ').strip('@') in a_perfis:
                i = a_perfis.index(perfil)
                try:
                    dados_tema.iloc[contador, dados_tema.columns.get_loc('Degree')] = a_degree[i]
                    dados_tema.iloc[contador, dados_tema.columns.get_loc('Eigenvector')] = a_eigenvector[i]
                    dados_tema.iloc[contador, dados_tema.columns.get_loc('Modularidade')] = a_modularidade[i]
                    if fazendo_betweeness:
                        dados_tema.iloc[contador, dados_tema.columns.get_loc('Betweeness')] = a_betweeness[i]
                except Exception as erro:
                    # Acho que consegui consertar os erros, mas fica aqui o código caso volte a dar problema
                    conta_erro += 1
                    print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
                    print('-{}-'.format(erro))
                    print(traceback.format_exc())
                    print('deu erro com {}'.format(perfil))

            contador += 1

        dados_tema.to_csv('{}/dados tweets rede_{}.csv'.format(pasta, tema), index=False)

        print('\n///////////////\n\nDADOS CONSOLIDADOS\n\n///////////\n')

# ------------------------------------------------------------------------------------------- PROCESSAMENTO E RELATÓRIO

    arquivo_relatorio = open('{}/relatorio_redes_{}.txt'.format(pasta, tema), 'w', encoding='utf-8')

    relatorio = 'RESUMO DA ANÁLISE DE REDES\n\n' \
                'Tema: {}\n' \
                'Publicações analisadas: {}\n' \
                'Período: {} a {}\n' \
                'Perfis analisados: {}\n'.format(tema, dados_tema.shape[0], inicio, fim, len(grafo))

    secao = '\n=====================================================================================================\n'

    arquivo_relatorio.write(relatorio)
    print('---relatório início ok')

    # 1. Degree
    top_perfis = pd.DataFrame()
    top_perfis = dados_perfis_rede.sort_values('Degree', ascending=False)['Usuário'].head(top_mostrar)
    relatorio = relatorio + secao + '\nDEGREE\n\n'
    relatorio = relatorio + '\nTop {} usuários com maior grau de conexão:' \
                            '\n{}'.format(top_mostrar, top_perfis)
    arquivo_relatorio.seek(0)
    arquivo_relatorio.write(relatorio)
    print('---{}--relatório degree ok'.format(datetime.datetime.now()))

    # 2. Eigen
    top_perfis = pd.DataFrame()
    top_perfis = dados_perfis_rede.sort_values('Eigenvector', ascending=False)['Usuário'].head(top_mostrar)
    relatorio = relatorio + secao + '\nEIGENVECTOR (método - {})\n\n'.format(eigen_metodo)
    relatorio = relatorio + '\nTop {} usuários centrais segundo a metodologia Eigenvector:' \
                            '\n{}'.format(top_mostrar, top_perfis)

    arquivo_relatorio.seek(0)
    arquivo_relatorio.write(relatorio)
    print('---{}--relatório eigen ok'.format(datetime.datetime.now()))

    # 3. Betweeness
    if fazendo_betweeness:
        top_perfis = pd.DataFrame()
        top_perfis = dados_perfis_rede.sort_values('Betweeness', ascending=False)['Usuário'].head(top_mostrar)
        relatorio = relatorio + secao + '\nBETWEENESS\n\n'
        relatorio = relatorio + '\nTop {} usuários centrais segundo a metodologia Betweeness:' \
                                '\n{}'.format(top_mostrar, top_perfis)

        arquivo_relatorio.seek(0)
        arquivo_relatorio.write(relatorio)
        print('---{}--relatório betweeness ok'.format(datetime.datetime.now()))

    # 4. Modularidade
    clusterizado = dados_perfis_rede.groupby(by='Modularidade', as_index=False).count()\
        .sort_values(by='Usuário', ascending=False)
    comu_total = clusterizado.shape[0]
    clusterizado = clusterizado[clusterizado['Usuário'] >= perfis_minimo]

    relatorio = relatorio + secao + '\nCOMUNIDADES\n\n'
    relatorio = relatorio + '\n-Foram detectadas {} comunidades, sendo {} com no mínimo {} membros.\n(Resolução = {})'\
        .format(comu_total, clusterizado.shape[0], perfis_minimo, resolucao)
    relatorio = relatorio + '-Medida de centralidade usada para as comunidades: {}\n'.format(medida_de_centralidade)

    # Perfil de cada comunidade
    for cluster in clusterizado['Modularidade']:
        dados_cluster = dados_perfis_rede[dados_perfis_rede['Modularidade'] == cluster]
        dados_cluster_tweets = dados_tema[dados_tema['Modularidade'] == cluster]

        print('------comunidade {}------'.format(cluster))
        print(dados_cluster.info())
        dados_cluster = dados_cluster.sort_values(by=medida_de_centralidade, ascending=False)

        relatorio = relatorio + secao + '\n----COMUNIDADE {}----\n'.format(cluster)
        relatorio = relatorio + 'Perfis: {}\n'.format(dados_cluster.shape[0])
        relatorio = relatorio + 'Total de publicações: {}\n'.format(dados_cluster_tweets.shape[0])
        relatorio = relatorio + 'Engajamento total: {}\n'.format(dados_cluster['Engajamento'].sum())

        relatorio = relatorio + '\n\n\n*****Top {} perfis centrais*****'.format(top_mostrar_2)

        # Infos dos principais usuários
        for perfil_ in dados_cluster['Usuário'].head(top_mostrar_2):
            dados_tweets_do_perfil = dados_cluster_tweets[dados_cluster_tweets['Usuário'] == perfil_]
            relatorio = relatorio + '\n\n---------------------------------------------'
            relatorio = relatorio + '\n@{}\n'.format(perfil_)

            # Principal publicação
            if dados_tweets_do_perfil.shape[0] > 0:
                relatorio = relatorio + 'Seguidores: {}\n'.format(dados_tweets_do_perfil.iloc[0]['Seguidores'])
                relatorio = relatorio + '{}: {}\n'\
                    .format(medida_de_centralidade, dados_tweets_do_perfil.iloc[0][medida_de_centralidade])
                relatorio = relatorio + 'Publicações: {}\n'.format(dados_tweets_do_perfil.shape[0])
                relatorio = relatorio + 'Engajamento: {}\n'.format(dados_tweets_do_perfil['Engajamento'].sum())
                top_tweets = dados_tweets_do_perfil.sort_values(by='Engajamento', ascending=False)
                relatorio = relatorio + '\nPublicação com mais engajamento:\n{}\n'.format(top_tweets.iloc[0]['Url'])
                relatorio = relatorio + '{} às {}\n'.format(top_tweets.iloc[0]['Data'], top_tweets.iloc[0]['Hora'])
                relatorio = relatorio + '{}\n'.format(top_tweets.iloc[0]['Texto'])
            else:
                relatorio = relatorio + 'Publicações: {}\n'.format(dados_tweets_do_perfil.shape[0])

            # X PUBLICAÇÕES COM MAIS ENGAJAMENTO DA COMUNIDADE Y

        dados_cluster.to_csv('{}/dados cluster {} - {}.csv'.format(pasta, cluster, tema))

    arquivo_relatorio.seek(0)
    arquivo_relatorio.write(relatorio)
    print('---relatório modularidade ok {}'.format(datetime.datetime.now()))

    # Fim do relatório
    tempo_final_loop = datetime.datetime.now()
    relatorio = relatorio + secao + 'Inicio da análise: {}'.format(tempo_inicial_loop)
    relatorio = relatorio + '\nFim da análise: {}'.format(tempo_final_loop)
    relatorio = relatorio + '\nDuração da análise: {}'.format(tempo_final_loop - tempo_inicial_loop)

    arquivo_relatorio.seek(0)
    arquivo_relatorio.write(relatorio)
    print('---{}--relatório fim ok'.format(datetime.datetime.now()))

# ------------------------------------------------------------------------------------------------- DESENHO DO GRAFO

    if fazendo_figura:
        print('\n/-*/-*/-*/-*/-*/-*/-*/-*/\n Fazendo figura')
        nx.draw(grafo)
        plt.savefig('{}/grafo_{}'.format(pasta, tema))
        print('\n/-*/-*/-*/-*/-*/-*/-*/-*/\n Figura ok')

# ------------------------------------------------------------------------------------------------- FIM DO LOOP

print('\n\n-------------------FIM--------------------------')
tempo_final = datetime.datetime.now()
print('Início: {}'.format(tempo_inicial))
print('Fim: {}'.format(tempo_final))
print('Duração total: {}'.format(tempo_final - tempo_inicial))
