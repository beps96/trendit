#!/bin/bash

echo "Termina de Generar la lista"

echo "Comienza la busqueda"

cat lista.txt | xargs -n 1 ./buscresp.sh

echo "Termina la busqueda"
