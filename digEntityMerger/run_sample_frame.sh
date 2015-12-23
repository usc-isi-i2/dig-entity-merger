~/projects/spark-1.3.0/bin/spark-submit \
     --master local[*] \
     --executor-memory=4g \
    --driver-memory=4g \
    --py-files ~/projects/dig-entity-merger/digEntityMerger/merger.zip \
    ~/projects/dig-entity-merger/digEntityMerger/framer.py \
    ~/projects/dig-entity-merger/sample-data-to-frame/sample-frame.json-ld \
    ~/projects/dig-entity-merger/sample-data-to-frame/sample-frame-rdd.json \
    ~/projects/dig-entity-merger/sample-data-to-frame/frameddddddd text
