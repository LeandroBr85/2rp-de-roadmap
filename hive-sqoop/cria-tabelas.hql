CREATE TABLE IF NOT EXISTS generation_Leandro
( generation int, date_introduced date)
stored as orc;


CREATE TABLE IF NOT EXISTS pokemon_Leandro
(idnum int, 
name string,  
hp int, 
speed int,
attack int, 
special_attack int, 
defense int,
special_defense int, 
generation int)
stored as orc;