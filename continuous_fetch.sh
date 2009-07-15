#!/bin/bash

URLS_PER_ROUND=1000
WEBTABLE=webtable
# NUTCH=./bin/nutch
NUTCH=/root/nutch/bin/nutch

i=1
while [ 1 ]
do
    echo "Starting a round of fetching: # $i"
    echo "  - Generating $URLS_PER_ROUND urls."
    $NUTCH org.apache.nutchbase.crawl.GeneratorHbase $WEBTABLE -topN $URLS_PER_ROUND
    echo "  - Fetching them from the interwebs."
    $NUTCH org.apache.nutchbase.fetcher.FetcherHbase $WEBTABLE
    echo "  - Parsing and Updating the HBase table."
    $NUTCH org.apache.nutchbase.parse.ParseTable $WEBTABLE
    $NUTCH org.apache.nutchbase.crawl.UpdateTable $WEBTABLE
     i=$( expr $i + 1 )
done
