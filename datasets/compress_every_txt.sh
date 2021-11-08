#!/bin/bash

for folder in */; do
	echo ${folder}
	bzip2 -z -k ${folder}/*.txt
done
