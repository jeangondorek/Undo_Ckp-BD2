import json

# Função responsael por ler metadado.json e criar a tabela
# Todo inicio do programa ele reseta a tabela para o valor do metadado
def create_table_metadata(connection):
    try:
        # Cria um cursor para executar comandos SQL no banco de dados.
        cursor = connection.cursor()

        # Lê os metadados da tabela a partir de um arquivo JSON.
        with open("./src/data/metadado.json", "r") as metadata_file:
            metadata = json.load(metadata_file)

        # Define o nome da tabela.
        table_name = 'data'

        # Cria a lista de colunas com seus tipos de dados.
        columns = [f"{column} INT" for column in metadata["table"].keys()]

        # Define o comando SQL para remover a tabela se ela já existir.
        drop_table_sql = f"DROP TABLE IF EXISTS {table_name};"

        # Define o comando SQL para criar a tabela com as colunas especificadas.
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)});"

        # Executa os comandos SQL para remover e criar a tabela.
        cursor.execute(drop_table_sql)
        cursor.execute(create_table_sql)

        # Obtém os nomes das colunas da tabela.
        column_names = ", ".join(metadata["table"].keys())

        # Obtém os valores a serem inseridos na tabela.
        values = ", ".join([f"({', '.join(map(str, row))})" for row in zip(*metadata["table"].values())])

        # Define o comando SQL para inserir os valores iniciais na tabela.
        insert_initial_values_sql = f"INSERT INTO {table_name} ({column_names}) VALUES {values};"

        # Executa o comando SQL para inserir os valores iniciais.
        cursor.execute(insert_initial_values_sql)

        # Comita as alterações no banco de dados.
        connection.commit()

    except Exception as error:
        print("Erro ao criar a tabela e inserir os valores iniciais:", error)
