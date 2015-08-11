dig-entity-merger
==================

Utility to link two datasets using the results from entity linking


Requirements:
-------------
* Spark: Visit http://spark.apache.org/downloads.html, select the package type of “Pre-built for Hadoop 2.4 and later,” and then click on the link for “Download Spark” This will download a compressed TAR file, or tarball. Uncompress the file into ```<spark-folder>```.

* Run `./make-spark.sh` every time to build the zip files required by spark every time you pull in new code


Running Entity Merger:
---------------------
The entity merger takes as input 3 data sources:
1. The input data source. This should be a JSON document example: `http://schema/doc/A  {"a": "Document", "hasElements": {"hasChild": {"a": "Person", "name":"X", "uri":"http://schema/person/X"}}, "uri": "http://schema/doc/A"}`
   Assume that the `Person` in the document has been linked to a person in a base datasource.
2. The base data source. This should have the uri/id of the data and a JSON document representing the data. Example:
`http://schema/person/standardA {"a": "Person", "source": "standard", "uri": "http://schema/person/standardA", "name": "Standard A"}`
3. The linking results. These link source 1 to source 2. These have the format:
`{"uri": "http://schema/person/X", matches: [{"score":1.0, "uri": "http://schema/person/standardA"}]}`

```
merger.py inputFile inputFileFormat baseFile baseFileFormat linkingResultFile outputDir outputFormat pathToJSONObjectInInput commaSepAttributesToRemoveOnMerge
```

Example Invocation:
```
cd <spark-folder>
./bin/spark-submit  \
   --master local[*]   \
   --executor-memory=4g  --driver-memory=4g \
   --py-files ~/github/dig-entity-merger/digEntityMerger/merger.zip \
   ~/github/dig-entity-merger/digEntityMerger/merger.py \
   hdfs://memex-nn1:8020/user/jslepicka/google_patents_assignments_reduced/part-r-00000 sequence \
   hdfs://memex-nn1:8020/user/jslepicka/corporation_wiki_processed/part-r-00000 sequence \
   ~/github/dig-entity-merger/sampleLinking.txt \
   ~/github/dig-entity-merger/result sequence \
   assignee.assignee 'name,address'
```

The input and output formats of the file can be text or sequence. Above example shows the input and output being sequence files.
The example below shows the input and output be text files:

```
cd <spark-folder>
./bin/spark-submit  \
   --master local[*]   \
   --executor-memory=4g  --driver-memory=4g \
   --py-files ~/github/dig-entity-merger/digEntityMerger/merger.zip \
   ~/github/dig-entity-merger/digEntityMerger/merger.py \
   ~/github/dig-entity-merger/sampleInput.txt text \
   ~/github/dig-entity-merger/sampleBase.txt text \
   ~/github/dig-entity-merger/sampleLinking.txt \
   ~/github/dig-entity-merger/result text \
   assignee.assignee 'name,address'
```