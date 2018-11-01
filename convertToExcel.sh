#!/bin/bash

for file in Database*
do
	unoconv --format xls "$file"
done
