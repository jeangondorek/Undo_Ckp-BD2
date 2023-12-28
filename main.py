from src.conn import conn
from src.database import create_table_metadata
from src.log_undo import read_log

# Conjunto para acompanhar transações já commitadas
transacoes_commitadas = set()

# Função principal do programa
def main():
    connection = None
    # Preciso que seja removido os espços dentro da transação
    # Ex: <T2,2, B,20>(errado) >> <T2,2,B,20>(correto) espaços que foram fonecidos no log ao lado da letra que idendifica a coluna A/B	
    vardolog = "./src/data/log.txt"
    try:
        # Estabelece uma conexão com o banco de dados
        connection = conn()
        # Desabilita transações implícitas para uso do CREATE TABLE
        connection.autocommit = True  

        # Cria a tabela de metadados no banco de dados
        create_table_metadata(connection)

        # Lê o log e realiza undo se necessário
        # Imprime valores da tabela incial, final e alterações feitas(inclusive as que realizaram UNDO) 
        # Porém as transações que realizam UNDO são impressas como modificações mas após é verificado que ela não deveria gravar
        # Então é voltado a alteração dela
        # No log.txt o exemplo é que T4 e T3 deram UNDO mas, T4 fez a alteração de uma das linhas e isso será impresso pelo programa
        # Mas, não ficara salvo no banco de dados final devido a realizar UNDO logo após a modificação
        # Seria mais para auditar o valor q a transação com UNDO tentou gravar e se gravou ou não
        read_log(vardolog)

    except Exception as error:
        print("Erro durante a execução:", error)
    finally:
        # Fecha a conexão se estiver aberta
        if connection and not connection.closed:
            connection.close()

if __name__ == "__main__":
    main()
