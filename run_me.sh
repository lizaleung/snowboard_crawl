#!/bin/bash

cd /Users/lizaleung/Documents/Github/snowboard_crawl
source ./env_dev
PATH=$PATH:/usr/local/bin
export PATH

/Library/Frameworks/Python.framework/Versions/3.7/bin/python3 -m scrapy crawl evo
touch "last_ran_$(date +%F)"
