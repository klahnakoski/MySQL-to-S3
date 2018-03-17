#!/usr/bin/env bash

# FOR USE ON THE MANAGER MACHINE

cd ~/MySQL-to-S3
export PYTHONPATH=.:vendor
python mysql_to_s3/extract.py --settings=resources/config/treeherder.json  >& /dev/null < /dev/null

