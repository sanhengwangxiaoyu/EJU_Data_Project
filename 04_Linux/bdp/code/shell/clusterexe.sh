#!/bin/bash
if [ ! -n "$1" ] ;then
    echo -e "\e[1;33m you have not input a word! \e[0m"
else
    echo -e "\e[1;33m node003 \e[0m"
    ssh root@node003 "$1 $2 $3 $4"
    echo -e "\e[1;33m node002 \e[0m"
    ssh root@node002 "$1 $2 $3 $4"
    echo -e "\e[1;33m node001 \e[0m"
    $1 $2 $3 $4
fi
