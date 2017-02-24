#!/bin/bash


rm -rf ./build
rm -rf ./dist
rm -rf ./sqlalchemy_drill.egg-info

git pull

pip3 install -r requirements/common.txt
python3 setup.py install

