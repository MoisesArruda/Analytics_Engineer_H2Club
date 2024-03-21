CREATE TABLE IF NOT EXISTS dim.cliente(
id INT PRIMARY KEY NOT NULL,
sexo CHAR(1),
data_nascimento DATE ,
data_cadastro TIMESTAMP,
cidade VARCHAR(50) NOT NULL,
sigla CHAR(2) NOT NULL
);