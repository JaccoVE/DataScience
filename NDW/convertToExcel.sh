#!/bin/bash

for file in Tableau\ Input\ Files/*
do
	unoconv --format xls "$file"
done
