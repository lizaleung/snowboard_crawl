#!/bin/bash

cd /Users/lizaleung/Documents/Github/snowboard_crawl
source ./env_dev
PATH=$PATH:/usr/local/bin
export PATH
/opt/local/bin/python3 -m scrapy crawl evo  > /tmp/stdout.log 2>/tmp/stderr.log


