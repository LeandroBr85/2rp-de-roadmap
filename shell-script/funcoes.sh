#!/bin/bash

lista_arquivos(){ 
shopt -s globstar nullglob
array=( **/*"$input"* )
 }

insere_texto(){ 
echo "$1" >> "$2"
}

insere_texto $1 $2
