# Importação das bibliotecas necessárias
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import os
import numpy as np
from datetime import date
from datetime import timedelta
from datetime import datetime
import hashlib
from pymongo import UpdateOne
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

def acessar_planilha_gsheets():
    # Configurações para autenticação com o Google Sheets
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    client = gspread.authorize(creds)
    planilha = client.open("Orçamento - João")
    return planilha

def gerar_id_unico(linha):
    # Converte os dados para string e gera um código MD5
    string_base = f"{linha['Data']}{linha['Descrição']}{linha['Valor']}"
    return hashlib.md5(string_base.encode('utf-8')).hexdigest()

def categorizar_despesas(despesas): # PENDENTE Criar o código para obter o caminho dentro do GitHub
    script_path = os.path.dirname(os.path.abspath(__file__))
    mapping_path = os.path.join(script_path, 'cat_mapping.csv')

    # Armazenamos o dataframe
    mapping = pd.read_csv(mapping_path, sep=';', decimal='.', dtype=str)

    # Criamos um dicionário a partir da tabela de correspondência
    mapping_dict = mapping.set_index('Descrição')['Categoria'].to_dict()

    # Criamos a coluna de categoria com base no mapeamento
    despesas['Categoria'] = despesas['Descrição'].map(mapping_dict)

def gerar_dict_datas(ano):
    if ano < 2016:
        return {
            'Janeiro': f'01/01/{ano}',
            'Fevereiro': f'01/02/{ano}',
            'Março': f'01/03/{ano}',
            'Abril': f'01/04/{ano}',
            'Maio': f'01/05/{ano}',
            'Junho': f'01/06/{ano}',
            'Julho': f'01/07/{ano}',
            'Agosto': f'01/08/{ano}',
            'Setembro': f'01/09/{ano}',
            'Outubro': f'01/10/{ano}',
            'Novembro': f'01/11/{ano}',
            'Dezembro': f'01/12/{ano}'
        }
    else:
        return {
            'JAN': f'01/01/{ano}',
            'FEV': f'01/02/{ano}',
            'MAR': f'01/03/{ano}',
            'ABR': f'01/04/{ano}',
            'MAI': f'01/05/{ano}',
            'JUN': f'01/06/{ano}',
            'JUL': f'01/07/{ano}',
            'AGO': f'01/08/{ano}',
            'SET': f'01/09/{ano}',
            'OUT': f'01/10/{ano}',
            'NOV': f'01/11/{ano}',
            'DEZ': f'01/12/{ano}'
        }

def extrair_2014_2015(ano):
    #Conectar a planilha
    planilha = acessar_planilha_gsheets()
    
    # Acessar a aba de 2014
    plan = planilha.worksheet(str(ano))

    # Obter os cabeçalhos da planilha de 2014
    headers = plan.row_values(1)

    # Filtrar os cabeçalhos para remover os vazios
    headers = [h for h in headers if h.strip()]

    # importar os dados da planilha de 2014 somente dos cabeçalhos válidos
    data = plan.get_all_records(expected_headers=headers)
    df = pd.DataFrame(data)

    # Criamos uma lista vazia que irá abrigar os subconjuntos criados no loop
    subsets = []

    # Criamos também um dicionário que relaciona as labels de meses do cabeçalho
    # com as datas que queremos inserir na coluna de data
    map_datas = gerar_dict_datas(ano)

    # Abrimos o loop definindo as variáveis e iremos iterar sobre os itens do dict
    for mes , data_despesa in map_datas.items():
        # Inicialmente verificamos se existe o mês do dict dentro do df de 2014
        if mes not in headers:
            continue

        # Criamos o primeiro subset selecionando a coluna de descrição e a
        # coluna do respectivo mês
        subset_df = df[[f'Despesas {ano}', mes]].copy()

        # Adicionamos a coluna de data no formato definido no dict
        subset_df['Data'] = data_despesa

        # Renomeamos as colunas para o padrão desejado
        subset_df.rename(columns={mes: 'Valor',f'Despesas {ano}':'Descrição'}, inplace=True)

        # Inserimos o subset dentro do contâiner criado anteriormente
        subsets.append(subset_df)

    # Transformamos a lista de dataframes num único dataframe para realizar
    # a limpeza dos dados
    despesas = pd.concat(subsets, ignore_index=True)

    # Vamos trocar o símbolo de decimal brasileiro para o americano
    # para consumo pela ferramenta de BI e deixamos a configuração de visualização
    # para a ferramenta de análise

    # Para a substituição primeiro convertemos os valores para string para que
    # possamos utilizar o método .replace() e depois convertemos de volta para número
    despesas['Valor'] = pd.to_numeric(
        despesas['Valor'].astype(str).str.replace(',', '.'),
        errors='coerce'
    )

    # Realizamos a limpeza da coluna valor
    despesas = despesas.dropna(subset=['Valor']) # Remove NA
    despesas = despesas[despesas['Valor'] != ''] # Remove vazios
    despesas = despesas[despesas['Valor'] != 0] # Remove zeros

    # Removemos as linhas onde temos descrição vazia
    despesas = despesas[despesas['Descrição'] != '']

    # Definimos o tipo de operação pelo sinal do valor
    despesas['Tipo'] = np.where(despesas['Valor'] < 0, 'D', 'R')

    # Convertemos de volta para valor para eliminação do sinal
    despesas['Valor'] = despesas['Valor'].astype(float).abs()

    # Categorizamos as despesas
    categorizar_despesas(despesas)

    # Criamos o id único para cada linha
    despesas['_id'] = despesas.apply(gerar_id_unico, axis=1)

    return despesas

def extrair_2016_2018(ano):
    #Conectar a planilha
    planilha = acessar_planilha_gsheets()
    
    #Importamos os dados de 2018 da planilha e criamos o DataFrame
    plan = planilha.worksheet(str(ano))
    data = plan.get_all_records(numericise_ignore=['all'])
    df = pd.DataFrame(data)

    #Vamos filtrar as linhas marcadas como cabeçalho utilizando a primeira
    #coluna e então remover a coluna pois não será mais necessária
    df = df.loc[df['C'] != 'C', :].iloc[:,1:]

    #Criamos a lista e o dict de datas para criar a coluna posteriormente
    subsets = []
    map_datas = gerar_dict_datas(ano)

    #O loop faz a divisão em subsets que são armazenados em uma lista
    for mes , data_despesa in map_datas.items():
        # Verificamos se o mês do dict existe entre as colunas do df de 2018
        if mes not in df.columns:
            continue

        #Encontramos o índice da coluna para usar de referência
        i_col = df.columns.get_loc(mes)
        subset_df = df.iloc[:, (i_col - 2):i_col + 1].copy()

        #Criamos a coluna de data utilizando o dict criado anteriormente
        subset_df['Data'] = data_despesa

        #Renomeamos as colunas para padronizar com os outros anos
        subset_df.rename(
            columns={mes : 'Valor',
                    f'DESC_{mes}':'Descrição',
                    f'C_{mes}':'Código'},
                    inplace=True
                    )

        #Armazenamos o subset na lista criada anteriormente
        subsets.append(subset_df)

    #Concatenamos a lista com os subsets em um único DataFrame
    despesas = pd.concat(subsets, ignore_index=True)

    #Convertemos a coluna de valor para numérico e tratamos os erros
    despesas['Valor'] = pd.to_numeric(
        despesas['Valor'].astype(str).str.replace(',', '.'),
        errors='coerce'
    )

    # Realizamos a limpeza da coluna valor
    despesas = despesas.dropna(subset=['Valor']) # Remove NA
    despesas = despesas[despesas['Valor'] != ''] # Remove vazios
    despesas = despesas[despesas['Valor'] != 0] # Remove zeros

    # Removemos as linhas onde temos descrição vazia
    despesas = despesas[despesas['Descrição'] != '']

    #Criamos a coluna de tipo para diferenciar receitas e despesas
    despesas['Tipo'] = np.where(despesas['Código'] == 'R', 'R', 'D')

    #Valores positivos com código de despesa 'D' são classificados como 'RE' (Reembolso)
    despesas['Tipo'] = np.where((despesas['Tipo'] == 'D') & (despesas['Valor'] > 0), 'RE', despesas['Tipo'])

    #Removemos a coluna de categoria pois não será mais necessária
    despesas = despesas.drop(columns=['Código'])

    #Transformamos os valores da coluna valor para positivos
    despesas['Valor'] = despesas['Valor'].abs()

    # Categorizamos as despesas
    categorizar_despesas(despesas)

    # Criamos o id único para cada linha
    despesas['_id'] = despesas.apply(gerar_id_unico, axis=1)

    return despesas

def extrair_2019_2025(ano):
    #Conectar a planilha
    planilha = acessar_planilha_gsheets()

    # Importamos os dados do ano da planilha e criamos o DataFrame
    plan = planilha.worksheet(str(ano))
    data = plan.get_all_records(numericise_ignore=['all'])
    df = pd.DataFrame(data)

    # Vamos filtrar as linhas marcadas como cabeçalho utilizando a primeira
    # coluna e então remover a coluna pois não será mais necessária
    df = df.loc[df['C'] != 'C', :].iloc[:,1:]

    # Criamos a lista e o dict de datas para criar a coluna posteriormente
    subsets = []
    map_datas = gerar_dict_datas(ano)

    # O loop faz a divisão em subsets que são armazenados em uma lista
    for mes , data_despesa in map_datas.items():

        # Verificamos se o mês do dict existe entre as colunas do df do ano
        if mes not in df.columns:
            continue

        # Encontramos o índice da coluna para usar de referência
        i_col = df.columns.get_loc(mes)
        subset = df.iloc[:, (i_col - 2):i_col + 1].copy()

        # Criamos a coluna de data utilizando o dict criado anteriormente
        subset['Data'] = data_despesa

        # Renomeamos as colunas para padronizar com os outros anos
        subset.rename(
            columns={mes : 'Valor',
                    f'DESC_{mes}':'Descrição',
                    f'C_{mes}':'Código'},
                    inplace=True
                    )

        # Armazenamos o subset na lista criada anteriormente
        subsets.append(subset)

    # Concatenamos a lista com os subsets em um único DataFrame
    despesas = pd.concat(subsets, ignore_index=True)

    # Convertemos a coluna de valor para numérico e tratamos os erros
    despesas['Valor'] = pd.to_numeric(
        despesas['Valor'].astype(str).str.replace(',', '.'),
        errors='coerce'
    )

    # Realizamos a limpeza da coluna valor
    despesas = despesas.dropna(subset=['Valor']) # Remove NA
    despesas = despesas[despesas['Valor'] != ''] # Remove vazios
    despesas = despesas[despesas['Valor'] != 0] # Remove zeros

    # Realizamos a limpeza da coluna descrição
    despesas = despesas[despesas['Descrição'] != '']

    # Criamos a coluna de tipo para diferenciar receitas e despesas
    despesas['Tipo'] = np.where(despesas['Código'] == 'R', 'R', 'D')

    # Valores positivos com código de despesa 'D' são classificados como 'RE' (Reembolso)
    despesas['Tipo'] = np.where((despesas['Tipo'] == 'D') & (despesas['Valor'] < 0), 'RE', despesas['Tipo'])

    # Removemos a coluna de categoria pois não será mais necessária
    despesas = despesas.drop(columns=['Código'])

    # Transformamos os valores da coluna valor para positivos
    despesas['Valor'] = despesas['Valor'].abs()

    # Categorizamos as despesas
    categorizar_despesas(despesas)

    # Criamos o id único para cada linha
    despesas['_id'] = despesas.apply(gerar_id_unico, axis=1)

    return despesas

def extrair_pos_2026(ano):
    #Conectar a planilha
    planilha = acessar_planilha_gsheets()

    # Criamos o contâiner que irá armazenar os subsets
    subsets = []
    map_datas = gerar_dict_datas(ano)

    # Importamos os dados do ano da planilha e criamos o DataFrame
    plan = planilha.worksheet(str(ano))
    data = plan.get_all_records(numericise_ignore=['all'])
    raw = pd.DataFrame(data)

    # Vamos filtrar as linhas marcadas como cabeçalho utilizando a primeira
    # coluna e então remover a coluna pois não será mais necessária
    raw = raw.loc[raw['C'] != 'C', :].iloc[:,1:]

    # O loop faz a divisão em subsets que são armazenados em uma lista
    for mes , data_despesa in map_datas.items():

        # Encontramos o índice da coluna para usar de referência
        i_col = raw.columns.get_loc(mes)
        subset_df = raw.iloc[:, (i_col - 3):i_col + 1].copy()

        # Criamos a coluna de data utilizando o dict criado anteriormente
        subset_df['Data'] = data_despesa

        # Renomeamos as colunas para padronizar com os outros anos
        subset_df.rename(
            columns={mes : 'Valor',
                    f'DESC_{mes}':'Descrição',
                    f'C_{mes}':'Código',
                    f'CAT_{mes}':'Categoria'
                    },
                        inplace=True
                        )
        # Armazenamos o subset na lista criada anteriormente
        subsets.append(subset_df)

    # Concatenamos a lista com os subsets em um único DataFrame
    despesas = pd.concat(subsets, ignore_index=True)

    # Convertemos a coluna de valor para numérico e tratamos os erros
    despesas['Valor'] = pd.to_numeric(
        despesas['Valor'].astype(str).str.replace(',', '.'),
        errors='coerce'
    )

    # Realizamos a limpeza da coluna valor
    despesas = despesas.dropna(subset=['Valor']) # Remove NA
    despesas = despesas[despesas['Valor'] != ''] # Remove vazios
    despesas = despesas[despesas['Valor'] != 0] # Remove zeros

    # Realizamos a limpeza da coluna descrição
    despesas = despesas[despesas['Descrição'] != '']

    # Criamos a coluna de tipo para diferenciar receitas e despesas
    despesas['Tipo'] = np.where(despesas['Código'] == 'R', 'R', 'D')

    # Valores positivos com código de despesa 'D' são classificados como 'RE' (Reembolso)
    despesas['Tipo'] = np.where((despesas['Tipo'] == 'D') & (despesas['Valor'] < 0), 'RE', despesas['Tipo'])

    # Removemos a coluna de código pois não será mais necessária
    despesas = despesas.drop(columns=['Código'])

    # Transformamos os valores da coluna valor para positivos
    despesas['Valor'] = despesas['Valor'].abs()

    # Criamos o id único para cada linha
    despesas['_id'] = despesas.apply(gerar_id_unico, axis=1)

    return despesas

def extrair_despesas_ano(ano):
    if ano < 2016:
        return extrair_2014_2015(ano)
    elif ano < 2019:
        return extrair_2016_2018(ano)
    elif ano < 2026:
        return extrair_2019_2025(ano)
    else:
        return extrair_pos_2026(ano)

def carregar_dados_mongodb(df):
    # Conexão com o MongoDB Atlas
    connection_string = os.environ.get("MONGO_URI")
    client = MongoClient(connection_string, server_api=ServerApi('1'))
    db = client['Dashboard_Financas_Pessoais']
    collection = db["movimentações"]

    # Adiciona um carimbo de tempo para auditoria (quando o dado foi processado)
    df['Processado_em'] = datetime.now()
    
    # Converte o DataFrame para o formato JSON/Dicionário que o Mongo entende
    registros = df.to_dict('records')
    
    # Prepara o lote de operações de Upsert
    operacoes = []
    for registro in registros:
        operacoes.append(
            UpdateOne(
                {"_id": registro['_id']}, # Procura pela chave primária (o hash)
                {"$set": registro},       # Se achar, atualiza com os dados mais recentes
                upsert=True               # Se não achar, insere como um novo documento
            )
        )
        
    # Executa a transação em lote no banco de dados
    if operacoes:
        try:
            resultado = collection.bulk_write(operacoes)
            print("\n📊 Resumo da Carga no MongoDB:")
            print(f" -> Documentos novos inseridos: {resultado.upserted_count}")
            print(f" -> Documentos atualizados: {resultado.modified_count}")
        except Exception as e:
            print(f"❌ Erro durante a carga dos dados: {e}")
    else:
        print("Nenhum dado novo para processar.")

# Fluxo de execução

if __name__ == "__main__":
    print("Iniciando Pipeline de Despesas Pessoais...\n")
    
    try:

        for ano in range(2014, date.today().year):
            print(f"Processando o ano de {ano}...")
            despesas = extrair_despesas_ano(ano)
            
            print(f"Enviando ano de {ano} para a nuvem...")
            carregar_dados_mongodb(despesas)
        
        print("\nPipeline finalizada com sucesso!")
        
    except Exception as erro_fatal:
        print(f"\nA pipeline falhou: {erro_fatal}")