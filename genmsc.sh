#!/bin/bash
Rscript mscconverter.R tmp.log
mscgen -T png -i sequence.txt
rm -f log.csv
rm -f sequence.txt
