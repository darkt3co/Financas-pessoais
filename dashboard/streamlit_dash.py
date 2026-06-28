import streamlit as st
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
from datetime import date

st.set_page_config(page_title="Dashboard Financeiro", page_icon="💸", layout="centered")

# Busca dos dados na VIEW com Cache de 10 minutos
@st.cache_data(ttl=600)
def buscar_dados_evolucao():
    try:
        uri = st.secrets["MONGO_URI"]
        client = MongoClient(uri)
        db = client["Dashboard_Financas_Pessoais"]
        
        # Consome a nova view que criamos
        colecao_view = db["cat_pormes_12meses"]
        dados = list(colecao_view.find())
        
        if not dados:
            return pd.DataFrame(columns=["Periodo", "Categoria", "Total_Gasto"])
            
        df = pd.DataFrame(dados)
        
        return df
    except Exception as e:
        st.error(f"Erro ao conectar ao MongoDB: {e}")
        return pd.DataFrame()

def buscar_dados_provisao():
    try:
        uri = st.secrets["MONGO_URI"]
        client = MongoClient(uri)
        db = client["Dashboard_Financas_Pessoais"]
        
        # Consome a coleção de provisões
        colecao = db["provisões"]
        dados = list(colecao.find())
                  
        df = pd.DataFrame(dados)
        
        return df
    except Exception as e:
        st.error(f"Erro ao conectar ao MongoDB: {e}")
        return pd.DataFrame()

# Título principal
st.title("Minhas despesas")

# Obtenção dos dados
df_evolucao = buscar_dados_evolucao()
df_provisao = buscar_dados_provisao()

if df_evolucao.empty | df_provisao.empty:
    st.warning("Nenhum dado encontrado na view ou erro de conexão.")
else:
    # Obtenção do mês e ano atual para filtrar as provisões
    mes_atual = datetime.today().month
    ano_atual = datetime.today().year

    # Conversão da coluna 'Data' para filtro
    df_provisao['Data'] = pd.to_datetime(df_provisao['Data']).dt.date

    # Subtítulo
    st.subheader("Provisões para o próximo mês")
    
    # Criamos o input de mês para seleção pelo usuário
    col1, col2 = st.columns([0.3,0.7])
    with col1:
        mes = st.number_input("Selecione o mês", min_value=1, max_value=12, value=mes_atual+1, disabled=False)
    df_provisao = df_provisao.loc[df_provisao['Data'] == date(ano_atual, mes, 1)]
    # st.dataframe(df_provisao[['Categoria','Data','Valor']],hide_index=True)
    met_col1, met_col2, met_col3 = st.columns(3)
    with met_col1:
        st.metric(
            label=df_provisao['Categoria'].iloc[0],
            value=df_provisao['Valor'].iloc[0],
            border=True)
        st.metric(
            label=df_provisao['Categoria'].iloc[3],
            value=df_provisao['Valor'].iloc[3],
            border=True)
        st.metric(
            label=df_provisao['Categoria'].iloc[4],
            value=df_provisao['Valor'].iloc[4],
            border=True)
    with met_col2:
        st.metric(
            label=df_provisao['Categoria'].iloc[1],
            value=df_provisao['Valor'].iloc[1],
            border=True)
        st.metric(
            label=df_provisao['Categoria'].iloc[4],
            value=df_provisao['Valor'].iloc[4],
            border=True)
        st.metric(
            label=df_provisao['Categoria'].iloc[6],
            value=df_provisao['Valor'].iloc[6],
            border=True)
    with met_col3:
        st.metric(
            label=df_provisao['Categoria'].iloc[2],
            value=df_provisao['Valor'].iloc[2],
            border=True)
                   
    # 2. Preparação do Dado para o Gráfico Nativo do Streamlit
    # O Streamlit exige que o eixo X seja o índice e as colunas sejam as categorias.
    # O comando pivot_table faz exatamente essa transformação (Matriz)
    df_pivot = df_evolucao.loc[~df_evolucao['Categoria'].isin(['Financeiro','Investimentos e Poupança','Receitas']),:].pivot_table(
        index="Periodo", 
        columns="Categoria", 
        values="Total_Gasto", 
        aggfunc="sum"
    ).fillna(0) # Preenche meses sem gastos na categoria com 0

    # 3. Exibição do Gráfico de Barras Empilhadas (Excelente para Mobile)
    st.subheader("Gastos Mensais por Categoria")
    st.bar_chart(df_pivot)

    # 4. Filtro Interativo (Opcional - Bom para detalhamento)
    st.subheader("Detalhamento por Mês")
    meses_disponiveis = sorted(df_evolucao["Periodo"].unique(), reverse=True)
    mes_selecionado = st.selectbox("Selecione o mês para analisar:", meses_disponiveis)

    # Filtra o dataframe baseado na escolha do usuário no celular
    df_mes = df_evolucao[df_evolucao["Periodo"] == mes_selecionado]
    
    # Formata para exibição em tabela
    df_tabela = df_mes[["Categoria", "Total_Gasto"]].copy()
    df_tabela["Total_Gasto"] = df_tabela["Total_Gasto"].apply(lambda x: f"R$ {x:,.2f}")
    
    st.dataframe(df_tabela, use_container_width=True, hide_index=True)