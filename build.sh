#!/bin/bash

venvfolder="bin include lib share"

for folder in ${venvfolder}
do
	[ -d ${folder} ] && rm -rf ${folder}
done

python3 -m virtualenv -p python3 ./
source ./bin/activate
./bin/python -m pip install --upgrade pip
./bin/pip install -r requirements.txt

