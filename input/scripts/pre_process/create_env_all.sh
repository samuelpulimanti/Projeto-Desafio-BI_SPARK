#!/bin/bash

# Criação das pastasssss

DADOS=("CLIENTES" "DIVISAO" "ENDERECO" "REGIAO" "VENDAS")

for i in "${DADOS[@]}"
do
	echo "Criando as pastas $i"
    mkdir ../../raw/$i
    chmod 777 ../../raw/$i
    cd ../../raw/ 
    curl -O https://raw.githubusercontent.com/samuelpulimanti/Projeto-Desafio-BI_SPARK/main/input/raw/$i.csv  
    hdfs dfs -mkdir /datalake/raw/$i
    hdfs dfs -chmod 777 /datalake/raw/$i
    hdfs dfs -copyFromLocal $i.csv /datalake/raw/$i
    beeline -u jdbc:hive2://localhost:10000/DESAFIO_CURSO -f ../../scripts/hql/create_table_$i.hql

done
