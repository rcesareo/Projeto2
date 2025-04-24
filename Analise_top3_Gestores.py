# -*- coding: utf-8 -*-


import pandas as pd
import argparse
import os
import glob
import sys

# Carregando dados do arquivo
def load_data(file_path = 'C:/Ranking de Gestao - 202402_valor.xls'):
    try:
        xls = pd.ExcelFile(file_path)
        sheet_names = xls.sheet_names
        data = pd.read_excel(file_path, sheet_name=sheet_names[2])
        print("Dados carregados com sucesso!")
        return data
    except Exception as e:
        print("Erro ao carregar os dados:", e)
        return None

#Pré processando dados para cálculo do ranking
def pre_processa(data):
    try:
        # Definir e limpar headers
        data_cleaned = data.drop(range(5)).reset_index(drop=True)
        data_cleaned.columns = data.iloc[4].values  
        data_cleaned = data_cleaned.iloc[:, 1:-1]
        data_cleaned = data_cleaned[~data_cleaned['Gestor'].str.contains("Total|Subtotal", na=False)]
        # Garantindo só numeros
        for col in data_cleaned.columns[1:]:
            data_cleaned[col] = pd.to_numeric(data_cleaned[col], errors='coerce')
        print("Dados limpos com sucesso!")
        return data_cleaned
    except Exception as e:
        print("Erro ao limpar os dados:", e)
        return None

#Processando os dados paa definir Top3
def find_top3_gestoras(data_cleaned):
    top_3_gestoras = {}
    segmentos = data_cleaned.columns[1:]
    for segmento in segmentos:
        try:
            top_3 = data_cleaned.nlargest(3, segmento)[['Gestor', segmento]]
            top_3_gestoras[segmento] = top_3
        except Exception as e:
            print(f"Erro ao processar o segmento {segmento}:", e)
    return top_3_gestoras

def find_xls(directory):
    # Encontrando o arquivo .xls mais recente na pasta do script
    os.chdir(directory)
    xls_files = glob.glob('*.xls')
    if not xls_files:
        return None
    file = max(xls_files, key=os.path.getmtime)
    return file

def main():
   
    # Recebendo o filepath por argumento    
    parser = argparse.ArgumentParser(description="Processa arquivo ambima de ranking de gestoras.")
    parser.add_argument("file_path", nargs='?', help="Caminho completo do arquivo de Ambima.")
    args = parser.parse_args()
   
   
    if hasattr(sys, 'frozen'):
       directory = os.path.dirname(sys.executable)
    elif '__file__' in globals():
        directory = os.path.dirname(os.path.abspath(__file__))
    else:
        directory = os.getcwd()
   
   
    # Validando argumento recebido caso no receba procura por xls mais recente
    file_path = args.file_path if args.file_path else find_xls(os.path.dirname(os.path.abspath(directory)))
    if not file_path:
        print("Arquivo não encontrado")
   
    file_path = file_path if file_path else 'C:/Ranking de Gestao - 202402_valor.xls'
    data = load_data(file_path)
    if data is not None:
        data_cleaned = pre_processa(data)
        if data_cleaned is not None:
            top_3_gestoras = find_top3_gestoras(data_cleaned)
            print(top_3_gestoras)

if __name__ == "__main__":
    main()
