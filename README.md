dig-entity-merger
==================

Utility to link two datasets using the results from entity linking


Requirements:
-------------
* Spark: Visit http://spark.apache.org/downloads.html, select the package type of “Pre-built for Hadoop 2.4 and later,” and then click on the link for “Download Spark” This will download a compressed TAR file, or tarball. Uncompress the file into ```<spark-folder>```.

* Run `./make-spark.sh` every time to build the zip files required by spark every time you pull in new code


Basic Entity Merger
-------------------
This takes as input 2 data sources:

1. The input data source. This should be a JSON document and can be a json lines text file or a sequence file.
Example:
```
http://test.com/ad/a {"a": "Advertisement", "uri":"http://test.com/ad/a", "hasGeo": {"location": {"uri":"http://geonames.org/CA/los_angeles"}}}
```

2. The base data source. This should have the uri/id of the data and a JSON document representing the data. This is the dataset which gets merged into the input
Example:
```
http://geonames.org/CA/los_angeles  {"a": "PostalAdress", "city":"Los Angeles", "uri":"http://geonames.org/CA/los_angeles", "state": "CA", "country": "USA"}
```

We need to let the merger know where the objects are in the input dataset that need to be joined with the base. In this example, the path in the input dataset would be `hasGeo.location`

<b>Invocation:</b>
```
basicMerger.py inputFile inputFileFormat inputJSONPath baseFile baseFileFormat outputDir outputFormat 
```

Example Invocation:
```
./bin/spark-submit \
     --master local[*] \
     --executor-memory=4g \
    --driver-memory=4g \
    --py-files ~/github/dig-entity-merger/digEntityMerger/merger.zip \
    ~/github/dig-entity-merger/digEntityMerger/basicMerger.py \
    --numPartitions 2 \
    ~/github/dig-entity-merger/sample-data/sample-ad.jl text hasGeo.location \
    ~/github/dig-entity-merger/sample-data/sample-geonames-locations.jl text \
    ~/github/dig-entity-merger/sample-data/basicMergerOutput text
```

Entity Merger using a Linking Results File:
-------------------------------------------
The entity merger takes as input 3 data sources:

1. The input data source. This should be a JSON document and can be a json lines text file or a sequence file.
Example:
```
http://test.com/ad/a {"a": "Advertisement", "uri":"http://test.com/ad/a", "hasGeo": {"location": {"uri":"http://test.com/address/los_angeles"}}}
```
We need to let the merger know where the objects are in the input dataset that need to be joined with the base. In this example, the path in the input dataset would be `hasGeo.location`


2. The base data source. This should have the uri/id of the data and a JSON document representing the data. This is the dataset which gets merged into the input
Example:
```
http://geonames.org/CA/los_angeles  {"a": "PostalAdress", "city":"Los Angeles", "uri":"http://geonames.org/CA/los_angeles", "state": "CA", "country": "USA"}
```

3. The linking results. These link source 1 to source 2. In this case, we need to tell the merger that the '"http://test.com/address/los_angeles" in input matches the "http://geonames.org/CA/los_angeles" in the base'. This is specified in the linking results file:
```
{"uri": "http://test.com/address/los_angeles", matches: [{"score":1.0, "uri": "http://geonames.org/CA/los_angeles"}]}
```


<b>Invocation:</b>
```
merger.py inputFile inputFileFormat inputPath baseFile baseFileFormat linkingResultFile outputDir outputFormat pathToJSONObjectInInput commaSepAttributesToRemoveOnMerge
```

Example Invocation:
```
./bin/spark-submit \
     --master local[*] \
     --executor-memory=4g \
    --driver-memory=4g \
    --py-files ~/github/dig-entity-merger/digEntityMerger/merger.zip \
    ~/github/dig-entity-merger/digEntityMerger/merger.py \
    --numPartitions 5 \
    ~/github/dig-entity-merger/sample-data/sample-ad-self-location.jl text hasGeo.location \
    ~/github/dig-entity-merger/sample-data/sample-geonames-locations.jl text \
    ~/github/dig-entity-merger/sample-data/sample-ad-linking-results.jl text \
    ~/github/dig-entity-merger/sample-data/mergerOutput text
```

Running Entity Deduplicator
---------------------------
After merging the entities, there could be duplicates in the dataset, or your input dataset could have duplicates to begin with.
The deduplicator remove duplicate objects at the given input path. For example, an Advertisement has contacts, and there could be duplicate contacts:
```
http://test.com/ad/a {"a": "Advertisement", "hasContact": {"people": [{"a": "Person", "name": "Joe"}, {"a": "Person", "name": "John"}, {"a": "Person", "name": "Joe"}]}, "uri": "http://test.com/ad/a"}
```
The contact Joe exists twice.

<b>Invocation:</b>
```
deduplicator.py inputFile inputFileFormat pathToJSONObjectInInput outputDir outputFormat
```

Example Invocation:
```
cd <spark-folder>
./bin/spark-submit  \
   --master local[*]   \
   --executor-memory=4g  --driver-memory=4g \
   --py-files ~/github/dig-entity-merger/digEntityMerger/merger.zip \
   ~/github/dig-entity-merger/digEntityMerger/deduplicator.py \
   ~/github/dig-entity-merger/sample-data/sample-duplicates.jl text hasContact.people \
   ~/github/dig-entity-merger/sample-data/dedupOutput text
```

Running Entity Cleaner
----------------------
Clean entities with a clean version from the base. If the uris match, it replaces the attributes the entity with the clean attributes from base.
For example consider an Ad with addresses:
```
http://test.com/ad/a {"a": "Advertisement", "hasGeo": {"location": {"a": "PostalAdress", "city": "Los Angeles", "state": "CA", "uri": "http://geonames.org/CA/los_angeles", "country": "USA"}}, "uri": "http://test.com/ad/a"}
```
Now it could be that there is a new dataset with better or cleaner locations:

```
http://geonames.org/CA/los_angeles   {"a": "PostalAdress", "city":"Los Angeles", "uri":"http://geonames.org/CA/los_angeles", "state": "CA", "country": "USA", "county": "Los Angeles County"}
```

Note that this one has the extra county information. The cleaner can be used to replace hasGeo.location from the input with this better data from the base.


<b>Invocation:</b>
```
cleaner.py inputFile inputFileFormat pathToJSONObjectInInput baseFile baseFileFormat outputDir outputFormat
```

Example Invocation:
```
cd <spark-folder>
./bin/spark-submit  \
   --master local[*]   \
   --executor-memory=4g  --driver-memory=4g \
   --py-files ~/github/dig-entity-merger/digEntityMerger/merger.zip \
   ~/github/dig-entity-merger/digEntityMerger/cleaner.py \
   --numPartitions 5 \
   ~/github/dig-entity-merger/sample-data/sample-ad-geonames.jl text hasGeo.location \
   ~/github/dig-entity-merger/sample-data/sample-geonames-locations-with-county.jl text \
   ~/github/dig-entity-merger/sample-data/cleanerOutput text
```