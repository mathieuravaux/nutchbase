#!/bin/bash
WEBTABLE=webtable
NUTCH=/root/nutch/bin/nutch
# SEED_PATH=/Users/Mathieu/Documents/Projects/Trylog/seed_nutchbase_reduced/
SEED_PATH=/root/url_seed/

echo "Creating the webtable ($WEBTABLE)..."
$NUTCH org.apache.nutchbase.util.hbase.WebTableCreator $WEBTABLE

echo "Injecting the urls in $SEED_PATH..."
$NUTCH org.apache.nutchbase.crawl.InjectorHbase $WEBTABLE $SEED_PATH

echo "done."
