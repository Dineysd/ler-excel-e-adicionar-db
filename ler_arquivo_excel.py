import sqlite3
import pandas as pd
import os
import hashlib

try:
    # Lê o arquivo Excel
    file_path = ''
    df = pd.read_excel(file_path)

    # Conecta ao banco de dados SQLite
    conn = sqlite3.connect('banco_de_dados.db')
    c = conn.cursor()

    # Função para remover acentuação e caracteres especiais
    def remover_acentos(texto):
        import unicodedata
        return ''.join(c for c in unicodedata.normalize('NFKD', texto) if unicodedata.category(c) != 'Mn')

    # Obtém o nome do arquivo sem o caminho
    file_name = os.path.basename(file_path)

    # Obtém o nome da tabela a partir do nome do arquivo até o "-"
    table_name = file_name.split('-')[0]

    # Sanitiza o nome da tabela
    table_name = remover_acentos(table_name).replace(" ", "_")

    # Obtém os nomes das colunas da primeira linha do arquivo e os sanitiza
    colunas = [remover_acentos(col).replace("/", "_").replace("-", "").replace(".", "").replace(" ", "_").lower() for col in df.columns.tolist()]

    # Adiciona a coluna id_hash
    colunas.append('id_hash')

    # Cria a tabela no SQLite, se não existir
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join([f'{col} TEXT' for col in colunas])})"
    print(create_table_query)
    c.execute(create_table_query)

    # Insere os dados na tabela
    for index, row in df.iterrows():
        values = [str(value) for value in row.tolist()]
        # Gera o hash da linha
        row_hash = hashlib.sha256(str(values).encode()).hexdigest()
        values.append(row_hash)
        # Verifica se o hash já existe na tabela antes de inserir
        c.execute(f"SELECT COUNT(*) FROM {table_name} WHERE id_hash=?", (row_hash,))
        if c.fetchone()[0] == 0:
            insert_query = f"INSERT INTO {table_name} ({', '.join(colunas)}) VALUES ({', '.join(['?']*len(colunas))})"
            print(insert_query)
            print(values)
            c.execute(insert_query, values)

    # Salva as alterações e fecha a conexão
    conn.commit()
    conn.close()
    
    print("Dados inseridos com sucesso.")

except Exception as e:
    print(f"Ocorreu um erro: {e}")
    if 'conn' in locals():
        conn.close()
