import streamlit as st
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

     

# Import the data
data = pd.read_csv('estoque.csv')

# Writing a title in page
st.title('Analise de Estoque')

st.write('Exemplo do artigo:')

def show_qtd_lines(df : pd.DataFrame):
    qtd_lines = st.sidebar.slider('Quantidade de linha máxima da consulta', min_value=1, max_value=len(df), step=1)
    st.write(df.head(qtd_lines).style.format(subset=['Valor'], formatter="{:.2f}"))

def plot_estoque(df: pd.DataFrame, categorie):
    data_plot = df.query('Categoria == @categorie')

    fig, ax = plt.subplots(figsize=(8,6))
    ax = sns.barplot(x = 'Produto', y = 'Quantidade', data = data_plot)
    ax.set_title(f'Quantidade em estoque dos produtos de {categorie}', fontsize=16)
    ax.set_xlabel('Produtos', fontsize = 12)
    ax.tick_params(rotation = 20, axis = 'x')
    ax.set_ylabel('Quantidade', fontsize = 12)

    return fig


# Filters 
checkbox_show_table = st.sidebar.checkbox('Mostrar tabela')
if checkbox_show_table:
    st.sidebar.markdown('## Filtro para a tabela')

    categories = list(data['Categoria'].unique())
    categories.append('Todas')

    categorie = st.sidebar.selectbox('Selecione a categoria desejada', options=categories)

    if categorie != 'Todas':
        df_categorie = data.query('Categoria == @categorie')
        show_qtd_lines(df_categorie)
    else:
        show_qtd_lines(data)

# Filter for graphic
st.sidebar.markdown('## Filtro para o gráfico')

categorie_graphic = st.sidebar.selectbox('Selecione a categoria para apresentar no gráfico', options = data['Categoria'].unique())
picture = plot_estoque(data, categorie_graphic)
st.pyplot(picture)


