#!/bin/bash

DADOS=("CLIENTES" "DIVISAO" "ENDERECO" "REGIAO" "VENDAS")

for i in "${DADOS[@]}"
do
	echo "$i"
    cd ../../raw/
	hdfs dfs -mkdir /datalake/raw/$i
    hdfs dfs -chmod 777 /datalake/raw/$i
    hdfs dfs -copyFromLocal $i.csv /datalake/raw/$i
    #beeline -u jdbc:hive2://localhost:10000/desafio_curso -f ../../scripts/hql/create_table_$i.hql

done
