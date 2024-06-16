-- creo y activo la base de datos
create database institute;
use institute;

-- creo una tabla
create table companies(
company_id varchar(10) primary key,
company_name varchar(100),
phone varchar(20),
email varchar(100),
country varchar(20),
website varchar(100)
);

-- intento insertar los datos del csv por código de diferentes maneras

-- muestra la ruta dónde está el sql
show variables where variable_name = 'datadir';
-- ubico el csv en esta ruta: /usr/local/var/mysql/ -- e intento cargar los datos
LOAD DATA INFILE '/usr/local/var/mysql/companies.csv'
INTO TABLE companies
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
IGNORE 1 LINES;
-- me sale un error de privacidad que intenté sortear de distintas maneras durante un día completo y fue imposible:
-- Error Code: 1290. The MySQL server is running with the --secure-file-priv option so it cannot execute this statement
-- Viendo imposible sortear la privacidad de mi Mac para insertar datos del csv por código,

-- inserto datos desde el menú "table data import wizard"
select * from companies;


create table credit_cards(
id varchar(10) primary key,
user_id varchar(5),
iban varchar(100),
pan	varchar(50),
pin varchar(20),
cvv varchar(20),
track1 varchar(100),
track2 varchar(100),
expiring_date varchar(20)
);
select * from credit_cards;

create table users_usa(
id int primary key,
name varchar(30),
surname varchar(30),
phone varchar(30),
email varchar(50),
birth_date varchar(30),
country varchar(30),
city varchar(30),
postal_code varchar(30),
address varchar(50)
);

select * from users_usa;

create table users_uk(
id int primary key,
name varchar(30),
surname varchar(30),
phone varchar(30),
email varchar(50),
birth_date varchar(30),
country varchar(30),
city varchar(30),
postal_code varchar(30),
address varchar(50)
);

select * from users_uk;

create table users_ca(
id int primary key,
name varchar(30),
surname varchar(30),
phone varchar(30),
email varchar(50),
birth_date varchar(30),
country varchar(30),
city varchar(30),
postal_code varchar(30),
address varchar(50)
);

-- para poder importarlo tuve que editar el csv Qébec por Qebec sin acento.
select * from users_ca;


create table users(
id int primary key,
name varchar(30),
surname varchar(30),
phone varchar(30),
email varchar(50),
birth_date varchar(30),
country varchar(30),
city varchar(30),
postal_code varchar(30),
address varchar(50),
tipo varchar(20)
);

-- inserto datos en users mediante una Uinion, agrego columna "tipo" que distinga el origen de datos
insert into users
select *, 'users_usa' as tipo from users_usa
union
select *, 'users_uk' from users_uk
union 
select *, 'users_ca' from users_ca;

select * from users;

drop table users_usa;
drop table users_uk;
drop table users_ca;

create table transactions(
id varchar(100) primary key,
card_id varchar(10),
business_id varchar(10),
fecha_hora timestamp,
amount decimal(20,2),
declined int,
product_ids varchar(100),
user_id int,
lat  varchar(100),
longitude varchar(100)
);
-- ALTER TABLE transactions MODIFY COLUMN amount DECIMAL(20,2);
alter table transactions
add foreign key(card_id) references credit_cards(id),
add foreign key(business_id) references companies(company_id),
add foreign key(user_id) references users(id);

desc transactions;
select * from transactions;

-- Índice para la tabla companies
CREATE INDEX idx_companies_id ON companies(company_id);

-- Índice para la tabla users
CREATE INDEX idx_users_id ON users(id);

-- Índice para la tabla credit_cards
CREATE INDEX idx_credit_cards_id ON credit_cards(id);


-- n1 ex1
select *, (select count(user_id) from transactions where transactions.user_id = users.id) as q_transactions
from users
where id in(
		select user_id 
        from transactions
        group by user_id
        having count(user_id) > 30
        order by count(user_id) desc
)
order by q_transactions desc;

-- agrego a los datos de la tabla users, la cantidad de transacciones por cada usuario 
-- y ordeno los resultados por esta columna de mayor a menor

-- considero tanto las transaciones declined como las no declined

-- n1 ex2
-- Muestra la media de amount por IBAN de las tarjetas de crédito en la compañía Donec Ltd., 
-- utiliza por lo menos 2 tablas.

select 
c.company_name, 
cr.iban, 
round(avg(t.amount),2)
from transactions t
inner join companies c on t.business_id = c.company_id
inner join credit_cards cr on cr.id = t.card_id
where c.company_name = 'Donec Ltd'
group by c.company_name, cr.iban;

-- considero tanto las transaciones declined como las no declined

-- n2 ex1

-- Crea una nueva tabla que refleje el estado de las tarjetas de crédito basado en si las últimas tres 
-- transacciones fueron declinadas y genera la siguiente consulta: ¿Cuántas tarjetas están activas?

-- paso 4: Creo la tabla de status, la estructura y luego la completo con la subquery que había generado
create table creditCardStatus ( 
card_id varchar(10) primary key,
estado varchar(20)
-- foreign key (card_id) references credit_cards(id) -- aquí no me lo permite
) as 
	-- Paso 3: Agrupo cada tarjeta por el estado, en una nueva columna estado
		select 
			card_id,
			-- opción con "if"
			if(sum(declined) < 3, 'activa', 'declinada') as estado
			-- opción con "case"
			-- case
			-- when sum(declined) = 0 then 'activa' 
			-- else 'declinada'
			-- end as estado
		from (
		-- Paso 2: Filtrar las tres transacciones más recientes para cada tarjeta de crédito
			select *
			from (
			-- Paso 1) numeramos las transacciones para cada tarjeta desde la mas reciente hasta la mas antigua, y
			-- creamos una nueva columna con esa numeración
				select 
				card_id,
				declined,
				row_number() over(partition by card_id order by fecha_hora) as row_num
				from transactions
			) as cronologic_transactions
			-- hasta aquí 1)
		where row_num <= 3
		) as last_3
		-- hasta aquí 2)
	group by card_id;
	-- hasta aquí 3)
-- hasta aquí 4)

-- paso 5 agrego clave foránea e indices para vincular tablas
-- La relación entre una tabla de tarjetas de crédito y una tabla de estatus de tarjetas es una relación de uno a uno. 
-- En una relación uno a uno, normalmente la clave foránea se coloca en la tabla que representa el detalle o la información adicional
-- La tabla credit_card_status depende de la existencia de registros en credit_cards.

alter table creditCardStatus
add foreign key (card_id) references credit_cards(id);

CREATE UNIQUE INDEX idx_card_id_unique ON creditCardStatus(card_id);

select * from CreditCardStatus;

-- paso 6) consulto las tarjetas caducadas
select estado, count(card_id)
from CreditCardStatus
where estado = 'activa';


-- n3 ex1

-- creamos la tabla products, estructura, inserción de datos y verificamos

drop table products;

create table products(
id varchar(20) primary key,
product_name varchar(100),
price varchar(30),
colour varchar(20),
weight varchar(10),
warehouse_id varchar(20)
);

select * from products;

-- creo la tabla intermedia entre transacciones y productos con ambos campos como PK y FK

drop table trans_products;

create table trans_products (
id_transaction varchar(100),
id_product varchar(20),
primary key (id_transaction, id_product),
foreign key (id_transaction) references transactions(id),
foreign key (id_product) references products(id));

-- lleno la tabla con los id de transacciones y productos, utilizando find_in_set

insert into trans_products (
id_transaction, id_product)
select t.id, p.id
from transactions t
inner join products p on find_in_set(p.id, replace(t.product_ids,', ',','))
where declined = 0;

-- comprobamos con una transacción si el funcionamiento es el esperado
select * from transactions where id = '02C6201E-D90A-1859-B4EE-88D2986D3B02';
select * from trans_products where id_transaction = '02C6201E-D90A-1859-B4EE-88D2986D3B02';

-- Ejercicio 1 Necesitamos conocer el número de veces que se ha vendido cada producto.

select p.id, p.product_name, count(id_transaction) as q_sold
from trans_products t
inner join products p on t.id_product = p.id
group by p.id, p.product_name
order by q_sold desc;
