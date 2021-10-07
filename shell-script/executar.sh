#!/bin/bash
source ./funcoes.sh #importa as funções 

lista_arquivos

insere_texto $1 $2

for ELEMENT in ${array[@]}; do if test ${ELEMENT:(-3)} = txt; then insere_texto "2rp" $ELEMENT; fi; done

