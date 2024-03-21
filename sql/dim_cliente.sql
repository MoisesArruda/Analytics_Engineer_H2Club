CREATE TABLE IF NOT EXISTS dim.cliente(
id INT PRIMARY KEY NOT NULL,
sexo CHAR(10),
data_nascimento DATE ,
data_cadastro TIMESTAMP,
cidade VARCHAR(50) NOT NULL,
sigla CHAR(20) NOT NULL
);