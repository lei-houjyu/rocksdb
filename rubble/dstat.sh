#!/bin/bash

mv dstat.csv old-dstat.csv

dstat -cdt -C 0,1,2,3,4,5,6,7 --output dstat.csv
