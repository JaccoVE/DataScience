#!/bin/bash

for file in dataFlow/*
do
	unoconv --format xls "$file"
done

for file in dataSpeed/*
do
	unoconv --format xls "$file"
done
