import mysql.connector
from mysql.connector import errorcode
import pandas as pd

df = pd.read_csv(r'/home/slapthatbass/Downloads/MICRODADOS.csv',sep=';',encoding='latin-1')

dim_Sexo = pd.DataFrame(df['Sexo'].unique(), columns=["Sexo"])
dim_RacaCor = pd.DataFrame(df['RacaCor'].unique(), columns=["RacaCor"])
dim_Municipio = pd.DataFrame(df['Municipio'].unique(), columns=["Municipio"])
dim_FaixaEtaria = pd.DataFrame(df['FaixaEtaria'].unique(), columns=["FaixaEtaria"])
fato_Obito = pd.DataFrame(df.dropna(subset=["DataObito"]))

print("Conectando...")
escolha = input(str("Deseja iniciar o abastecimento de dados do banco? ")).upper()

if escolha == 'SIM':
    try:
        conn = mysql.connector.connect(host='localhost', user='root', password='AlphaBeta')
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print('Existe algo errado no nome de usuário ou senha')
        else:
            print(err)

    cursor = conn.cursor()

    cursor.execute("DROP DATABASE IF EXISTS `COVID`;")

    cursor.execute("CREATE DATABASE `COVID`;")

    cursor.execute("USE `COVID`;")

    # criando tabelas
    TABLES = {}
    TABLES['DIM_RACA'] = ('''
            CREATE TABLE DIM_RACA (
            RACA VARCHAR (100) PRIMARY KEY NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin ''')

    TABLES['DIM_MUNICIPIO'] = ('''
            CREATE TABLE DIM_MUNICIPIO(
            NOME_MUNICIPIO VARCHAR (100) PRIMARY KEY NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin ''')

    TABLES['DIM_FAIXA_ETARIA'] = ('''
            CREATE TABLE DIM_FAIXA_ETARIA(
            FAIXA_ETARIA VARCHAR (100) PRIMARY KEY NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin ''')

    TABLES['DIM_SEXO'] = ('''
            CREATE TABLE DIM_SEXO(
            LETRA_SEXO VARCHAR (100) PRIMARY KEY NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin ''')

    TABLES['FATO_OBITO'] = ('''
            CREATE TABLE FATO_OBITO(
            ID_OBITO INT NOT NULL AUTO_INCREMENT PRIMARY KEY, RACA_FK VARCHAR (100) NOT NULL, 
            MUNICIPIO_FK VARCHAR (100) NOT NULL, FAIXA_ETARIA_FK VARCHAR (100) NOT NULL, 
            LETRA_SEXO_FK VARCHAR (100) NOT NULL, FOREIGN KEY (RACA_FK) REFERENCES DIM_RACA (RACA), 
            FOREIGN KEY (MUNICIPIO_FK) REFERENCES DIM_MUNICIPIO (NOME_MUNICIPIO), 
            FOREIGN KEY (FAIXA_ETARIA_FK) REFERENCES DIM_FAIXA_ETARIA (FAIXA_ETARIA), 
            FOREIGN KEY (LETRA_SEXO_FK) REFERENCES DIM_SEXO (LETRA_SEXO)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin ''')

    for tabela_nome in TABLES:
        tabela_sql = TABLES[tabela_nome]
        try:
            print('Criando tabela {}:'.format(tabela_nome), end=' ')
            cursor.execute(tabela_sql)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print('Já existe')
            else:
                print(err.msg)
        else:
            print('OK')

    dim_Raca_SQL = 'INSERT INTO DIM_RACA (RACA) VALUES (%s)'
    raca = dim_RacaCor.values.tolist()
    cursor.executemany(dim_Raca_SQL, raca)

    dim_Municipio_SQL = 'INSERT INTO DIM_MUNICIPIO (NOME_MUNICIPIO) VALUES (%s)'
    municipio = dim_Municipio.values.tolist()
    cursor.executemany(dim_Municipio_SQL, municipio)

    dim_Faixa_Etaria_SQL = 'INSERT INTO DIM_FAIXA_ETARIA (FAIXA_ETARIA) VALUES (%s)'
    faixa_Etaria = dim_FaixaEtaria.values.tolist()
    cursor.executemany(dim_Faixa_Etaria_SQL, faixa_Etaria)

    dim_Sexo_SQL = 'INSERT INTO DIM_SEXO (LETRA_SEXO) VALUES (%s)'
    sexo = dim_Sexo.values.tolist()
    cursor.executemany(dim_Sexo_SQL, sexo)
    
    fato_Obito_SQL = 'INSERT INTO FATO_OBITO (RACA_FK, MUNICIPIO_FK, FAIXA_ETARIA_FK, LETRA_SEXO_FK) VALUES (%s, %s, %s, %s)'
    obito = pd.DataFrame(fato_Obito[['RacaCor', 'Municipio', 'FaixaEtaria', 'Sexo']])
    aux = obito.values.tolist()
    cursor.executemany(fato_Obito_SQL, aux)

    # commitando se não nada tem efeito
    conn.commit()
    print("Commit bem sucedido.")
    cursor.close()
    conn.close()
else:
    print("Programa encerrado!")
