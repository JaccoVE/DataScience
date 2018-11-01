#!/bin/bash

for file in dataFlow/*
do
	unoconv --format xls "$file"
done

for file in dataSpeed/*
do
	unoconv --format xls "$file"
done

unoconv --format xls "dataRoad.csv"
unoconv --format xls "Location.csv"
unoconv --format xls "metaFlow.csv"
unoconv --format xls "metaRoad.csv"
unoconv --format xls "metaSpeed.csv"
