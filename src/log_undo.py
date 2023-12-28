import re
from src.conn import conn

# TODO: implementar reconhecimentos e execuções de rollback por exemplo

# Define expressões regulares para cada tipo de linha
# Como já dito, as transações foram alteradas no arquivo log para remover um espaço a mais evitando mais regex aqui
# a transação dever ser escrita sem espaços como no exemplo: <T1,1,A,11>
regex_transacao = r"<T\d+,\d+, .+,\d+>"
regex_start = r"<start T\d+>"
regex_commit = r"<commit T\d+>"
regex_ckpt = r"<START CKPT\(T\d+\)>"
regex_end_ckpt = r"<END CKPT>"

# Inicialização de listas e variáveis para acompanhar informações do log
checkpoint = False # Controle do checkpoint
commited = False # Controle de se foi commitada ou não

lista_transacoes_sem_commit = [] # Para transações que não commitaram
lista_start_sem_commit_e_sem_transacao = [] # Para transações que só deram start

t_start = [] # verifica trasações q tenha start

transacoes_processadas = set()  # Para acompanhar transações já processadas

transaction_sucess = [] # Transações que foram bem sucedidas, ou seja, tiver commit ou foram salvas pelo checkpoint

t_changes = {} # Transações que realiaram modificações para impressão das modificações no final
alteracoes = {} # Grava alterações para print das alterações

listadeundo = [] # Bagunça para conseguir executar a trasação e depois poder dar "rollback" dela, no caso, não salvar, dar UNDO

consultas_pendentes = [] # Grava consultas que ficaram pendetes, utilizado para verificação dos dados, mostrar auditar/conferir se tudo está ok

# Função para limpar os valores para inserir ou fazer update
def limpar_valor(valor):
    return ''.join(filter(str.isdigit, valor))

def read_log(vardolog):
    try:
        # estabelece conexão para esse arquivo, novamente porque sim
        connection = conn()
        connn = connection.cursor()

        # lê arquivo de logs e faz o arquivo json no formato dado ficar no formato do banco
        log_file = open(vardolog, "r", encoding="utf-8")
        log_lines = log_file.readlines()
        log_lines.reverse()
        
        # executa para conferir a inicialização do banco
        connn.execute("select * from data;")

        # Imprime situação inicial do banco de dados
        # Utilizado para ver as diferenças inicias e finais do banco
        results = connn.fetchall()
        print("Valores da tabela 'data' INITIAL:")
        for row in results:
            print(row)
        print('\n')

        # Inicia varedurra do log em busca das transações que se enquadram em cada váriavel
        for linha in log_lines:

            # Se a linha for de start, verifica se ela possui commit e possui transação
            if re.search(regex_start, linha):
                # funciona para casos onde inicia, tenha transação e não tenha commit
                transacao = re.search(r"<start T(\d+)>", linha).group(1)
                commit_encontrado = any(re.search(regex_commit, l) and f"T{transacao}" in l for l in log_lines)
                transacao_encontrada = any(re.search(regex_transacao, l) and f"T{transacao}" in l for l in log_lines)
                if not commit_encontrado and not transacao_encontrada and transacao not in transacoes_processadas:
                    lista_start_sem_commit_e_sem_transacao.append(linha)
                    # Adiciona à lista de transações processadas
                    transacoes_processadas.add(transacao) 

            # veerifica o checkpoint
            # basicamente ignora as transações que estejam dentro do checkpoint
            # pela explicação que solicitei, as regras para o log garantirão que ocorra de forma correta
            # como está no código as transações, UNDO e etc
            elif re.search(regex_ckpt, linha) and checkpoint:
                break
            elif re.search(regex_end_ckpt, linha):
                checkpoint = True
            else:
                continue

        # indica que foram realizados UNDOS nas trasações inciadas sem commit
        for index in lista_start_sem_commit_e_sem_transacao:
            transacao = re.search(r'<start (T\d+)>', index).group(1)
            numero_transacao = re.search(r'T(\d+)', transacao).group(1)
            listadeundo.append(numero_transacao)
            print("Transação {} realizou UNDO".format(transacao))
        
        print('\n')

        # faz update no banco com as transações, independente de undo ou não
        for index in lista_transacoes_sem_commit:
            valores = re.search(r"<.+>", index).group(0).replace("<", "").replace(">", "").split(", ")[1:]
            try:
                connn.execute(f"UPDATE data SET {valores[1]} = {valores[2]} WHERE id = {valores[0]};")
                connn.commit()
                transacao = re.search(r'<T(\d+),', index).group(1)
                alteracao = "UPDATE data SET {} = {} WHERE id = {};".format(valores[1], valores[2], valores[0])
                t_changes.setdefault(transacao, []).append(alteracao)

            except Exception as error:
                print("Erro ao realizar UNDO:", error)

        # ABRE NOVAMENTE O ARQUIVO LOG para fazer verificações e update no banco
        # assim como rollback
        with open(vardolog, 'r') as arquivo:
            # faz a verificação de alterações e valores alterados 
            for linha in arquivo:
                partes = linha.strip().split(',')

                # verifica se inicou transação
                match = re.search(r"<start T(\d+)>", linha)
                if match:
                    # Extrai o número da transação e adiciona à lista
                    transacao = int(match.group(1))
                    t_start.append(transacao)
                
                if len(partes) == 4:
                    transacao, identificador, nome, valor = partes

                    # Remove caracteres não numéricos do valor
                    valor_limpo = limpar_valor(valor)

                    if nome == 'A':
                        coluna = 'a'
                    elif nome == 'B':
                        coluna = 'b'
                    else:
                        # Nome inválido, ignore esta entrada
                        continue

                    transacao1 = int(re.search(r'T(\d+)', linha).group(1))
                    # Adicione a consulta à lista de consultas pendentes
                    consultas_pendentes.append((transacao1, coluna, valor_limpo, identificador))
                    
        listadeundo2 = [int(x) for x in listadeundo]

        # Executar as consultas SQL após a conclusão do loop, fora do loop for
        for transacao1, coluna, valor_limpo, identificador in consultas_pendentes:
            # Modifique a consulta SQL para selecionar a coluna desejada
            consulta_sql = f"SELECT {coluna} FROM data WHERE id = %s"
            connn.execute(consulta_sql, (identificador,))
            resultado = connn.fetchone()

            if resultado is not None:
                # O resultado é uma tupla, pegue o valor da coluna correta
                valor_original = resultado[0]
                
                # Execute a atualização
                # indiferente se deu undo ou não
                # A não ser que não tenha start, aí será ignorado
                if transacao1 not in t_start:
                    print('Transação ignorada T',transacao1, '\n')

                # pelas regras discutidas essa verificação se tornou inútil
                # ela verificaria caso houvesse possibilidade de alguma transação dentro do checkpoint não ter commit
                # com objetivo de reverter elas, porém essa regra não será necessária
                if transacao1 in t_start:
                    consulta_sql = f"UPDATE data SET {coluna} = %s WHERE id = %s"
                    connn.execute(consulta_sql, (valor_limpo, identificador))

                # deixa as transações que modificaram dados salvas para verificação.
                if transacao1 in alteracoes:
                    alteracoes[transacao1].append(f"alterou id {identificador} coluna {coluna} para {valor_limpo}")
                else:
                    alteracoes[transacao1] = [f"alterou id {identificador} coluna {coluna} para {valor_limpo}"]

                if transacao1 in listadeundo2:
                    # Execute a reversão da atualização apenas se a transação estiver em listadeundo2
                    # ou seja caso a trasação tenha dado UNDO
                    consulta_sql = f"UPDATE data SET {coluna} = %s WHERE id = %s"
                    connn.execute(consulta_sql, (valor_original, identificador))


        # printa alterações executadas mas não printa a volta delas, ae teria que analisar quem deu undo e desconsiderar, mas ver que houve essa transação no banco
        for transacao, alteracao in alteracoes.items():
            print(f"Transação {transacao} realizou as seguintes alterações:")
            for a in alteracao:
                print(a)

        # print final dos dados
        connn.execute("select * from data;")
        print('\n')
        # Recupere os resultados da consulta
        results = connn.fetchall()
        print("Valores da tabela 'data' FINAL:")
        for row in results:
            print(row)

        connection.commit()
        connn.close()
        connection.close()

    except Exception as error:
        print("Erro ao ler o log:", error)

if __name__ == "__main__":
    read_log()
