CREATE TABLE IF NOT EXISTS ft.resultado(
id INT NOT NULL,
data_acesso TIMESTAMP NOT NULL,
clientes_id INT NOT NULL,
buyin DECIMAL(18, 2),
rake DECIMAL(18, 2) NOT NULL,
winning DECIMAL(18, 2),
	
-- Adicionando a chave estrangeira para id_cliente referenciando dim.cliente
constraint fk_cliente
foreign key (clientes_id)
references dim.cliente(id)

)

