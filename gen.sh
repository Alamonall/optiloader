#!/bin/bash

NUM_LINES=125000000 # количество строк в файле
LINE_LENGTH=800     # длина строки в символах (в кодировке base64)

dd if=/dev/urandom bs=$LINE_LENGTH count=$NUM_LINES | base64 > largefile.txt
