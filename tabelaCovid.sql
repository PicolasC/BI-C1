DROP TABLE FATO_OBITO;
DROP TABLE DIM_RACA;
DROP TABLE DIM_MUNICIPIO;
DROP TABLE DIM_FAIXA_ETARIA;
DROP TABLE DIM_SEXO;
CREATE TABLE DIM_RACA (RACA VARCHAR (100) PRIMARY KEY NOT NULL);
CREATE TABLE DIM_MUNICIPIO (NOME_MUNICIPIO VARCHAR (100) PRIMARY KEY NOT NULL);
CREATE TABLE DIM_FAIXA_ETARIA (FAIXA_ETARIA VARCHAR (100) PRIMARY KEY NOT NULL);
CREATE TABLE DIM_SEXO (LETRA_SEXO VARCHAR (100) PRIMARY KEY NOT NULL);
CREATE TABLE FATO_OBITO (ID_OBITO INT NOT NULL AUTO_INCREMENT PRIMARY KEY, RACA_FK VARCHAR (100) NOT NULL,
                        MUNICIPIO_FK VARCHAR (100) NOT NULL, FAIXA_ETARIA_FK VARCHAR (100) NOT NULL,
                        LETRA_SEXO_FK VARCHAR (100) NOT NULL, FOREIGN KEY (RACA_FK) REFERENCES DIM_RACA (RACA),
                        FOREIGN KEY (MUNICIPIO_FK) REFERENCES DIM_MUNICIPIO (NOME_MUNICIPIO),
                        FOREIGN KEY (FAIXA_ETARIA_FK) REFERENCES DIM_FAIXA_ETARIA (FAIXA_ETARIA),
                        FOREIGN KEY (LETRA_SEXO_FK) REFERENCES DIM_SEXO (LETRA_SEXO));