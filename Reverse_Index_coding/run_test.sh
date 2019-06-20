#!/bin/bash
for (( c=4; c<=10; c++ ))
do
   python3 main.py $c 1 300
done
