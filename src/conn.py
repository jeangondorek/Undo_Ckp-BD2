import psycopg2
import configparser

def conn():
    try:
       # Lê as configurações do banco de dados a partir de 'env.ini' usando a biblioteca configparser.
        config = configparser.ConfigParser()
        config.read('env.ini')
        
        # Obtém as configurações específicas da seção 'postgresql' do arquivo de configuração.
        postgresql_section = config['postgresql']

        # Cria um dicionário com as configurações de conexão.
        db_config = {
            "user": postgresql_section['user'],
            "host": postgresql_section['host'],
            "database": postgresql_section['database'],
            "password": postgresql_section['password'],
            "port": postgresql_section['port']
        }

        # Tenta estabelecer uma conexão com o banco de dados usando as configurações fornecidas.
        connection = psycopg2.connect(**db_config)

        # Retorna a conexão estabelecida.
        return connection
        
    except Exception as error:
        # Em caso de erro, imprime uma mensagem de erro e levanta a exceção.
        print("Erro na conexão:", error)
        raise error
