#!/bin/bash

DIR="/home/megatron/workspace/"

var1=`du -sh $DIR | grep -o '^[^G|M|K]*'`
echo $var1