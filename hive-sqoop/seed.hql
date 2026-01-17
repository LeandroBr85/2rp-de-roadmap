INSERT INTO generation_leandro( generation, date_introduced)
VALUES (1,'1996-02-27'),
(2,'1999-11-21'),
(3,'2002-11-21'),
(4,'2006-09-28'),
(5,'2010-09-18'),
(6,'2010-12-13'),
(7,'2016-11-18');


# Inserir Pokemom.cvs no cluster no seu user, no HUE ir em hdfs >> user >> open in a browse >> carregar >> selecionar arquivo. 


CREATE EXTERNAL TABLE IF NOT EXISTS pokemon_le
(idnum int, 
name string,  
hp int, 
speed int,
attack int, 
special_attack int, 
defense int,
special_defense int, 
generation int)
stored as TEXTFILE;

LOAD DATA INPATH 'hdfs://bigdataclu-ns/user/2rp-leandror/pokemon1.csv' INTO TABLE pokemon_le;

#Não tenho acesso para carregar do hdfs no hive, foi usado outro procedimento para inserir a informação.

insert into pokemon_leandro
select * from pokemon_le;