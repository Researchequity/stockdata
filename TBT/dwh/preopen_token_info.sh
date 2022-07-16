#!/bin/bash

echo $1 $2
to=",${2},"
head -n 500000 $1| grep $to > $2".txt"

