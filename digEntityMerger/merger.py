#!/usr/bin/env python

from pyspark import SparkContext
from pyspark.sql import HiveContext, Row

from optparse import OptionParser
import json
from jsonUtil import JSONUtil


#https://issues.apache.org/jira/browse/SPARK-4851
#Am not able to make it a static method inside EntityMerger
def merge_json(input_jsons, merge_uri_and_jsons, input_path, removeElements):
        for x in input_jsons:
            input_json = x
            break

        for merge_uri_and_json in merge_uri_and_jsons:
            uri_and_jsons = []
            for x in merge_uri_and_json:
                uri_and_jsons.append(x)

            for uri_and_json in uri_and_jsons:
                input_json = JSONUtil.replace_values_at_path(input_json, input_path, uri_and_json[0],
                                                         uri_and_json[1], removeElements)
        return input_json

def load_linking_row(json_str, uri_as_key):
    json_obj = json.loads(json_str)

    if uri_as_key:
        key = json_obj["uri"]
        value = json_obj["matches"][0]["uri"]
    else:
        key = json_obj["matches"][0]["uri"]
        value = json_obj["uri"]
    return key, value

class EntityMerger:
    def __init__(self):
        pass

    # def merge_rdds(self, rdd1, path1, rdd2, remove_attr):
    #     hive_rdd1 = self.hiveCtx.jsonRDD(rdd1)
    #     hive_rdd1.printSchema()
    #     hive_rdd1.registerTempTable("a")
    #
    #     hive_rdd2 = self.hiveCtx.jsonRDD(rdd2)
    #
    #     source_uri = path1 + ".uri"
    #
    #     query = "SELECT * from a LATERAL VIEW OUTER explode(" + path1 + ") mergeTempTable AS tomatch"
    #     hive_rdd1_explode = self.hiveCtx.sql(query)
    #     hive_rdd1_explode.printSchema()
    #
    #     result = hive_rdd1_explode.join(hive_rdd2, hive_rdd1_explode.tomatch.uri == hive_rdd2.source, "left_outer")
    #     return result.toJSON()

    @staticmethod
    def merge_rdds(inputRDD, inputPath, baseRDD, joinRDD, removeElements):
        #1. Merge baseRDD and joinRDD
        #baseRDD: base_uri base_json
        #joinRDD: source_uri base_uri
        #output:  source_uri, base_json
        join_rdd_on_base = join_rdd.map(lambda x: load_linking_row(x, False))
        base_merge = join_rdd_on_base.join(base_rdd).map(lambda x: (x[1][0], x[1][1]))

        #2. Extract the source_uri from inputRDD
        #output: source_uri, input_uri
        input_source_rdd = inputRDD.flatMapValues(lambda x: JSONUtil.extract_values_from_path(x, inputPath)) \
            .map(lambda (x, y): (y, x))

        #3. JOIN extracted source_uri with base
        #output source_uri, (input_uri, base_json)
        merge3 = input_source_rdd.join(base_merge)

        #4. Make input_uri as the key
        #output: input_uri, (source_uri, base_json)
        merge4 = merge3.map(lambda (source_uri, join_res): (join_res[0], (source_uri, join_res[1])))

        #5. Group results by input_uri
        #output: input_uri, list(source_uri, base_json)
        merge5 = merge4.groupByKey()

        #6 Merge in input_json
        #output: input_uri, list(input_json), list(source_uri, base_json)
        merge6 = inputRDD.cogroup(merge5)

        #7 Replace JSON as necessary
        result = merge6.mapValues(lambda x: merge_json(x[0], x[1], inputPath, removeElements))
        return result


if __name__ == "__main__":
    """
        Usage: merger.py [inputFile1] [path to inner object] [inputFile2] [attributes to remove]
    """
    sc = SparkContext(appName="LSH-TOKENIZER")

    usage = "usage: %prog [options] inputDataset inputDatasetFormat" \
            "baseDataset baseDatasetFormat" \
            "joinResult outputFilename outoutFileFormat"
    parser = OptionParser()
    parser.add_option("-r", "--separator", dest="separator", type="string",
                      help="field separator", default="\t")
    # parser.add_option("-d", "--type", dest="data_type", type="string",
    #                   help="input data type: csv/json", default="csv")

    (c_options, args) = parser.parse_args()
    print "Got options:", c_options
    inputFilename1 = args[0]
    inputFileFormat1 = args[1]

    baseFilename = args[2]
    baseFormat = args[3]

    joinResultFilename = args[4]

    outputFilename = args[5]
    outputFileFormat = args[6]

    inputPath = args[7]
    removeElementsStr = args[8].strip()
    removeElements = []
    if len(removeElementsStr) > 0:
        removeElements = removeElementsStr.split(",")

    print "Write output to:", outputFilename
    if inputFileFormat1 == "text":
        input_rdd1 = sc.textFile(inputFilename1).map(lambda x:
                                                     (x.split(c_options.separator)[0], json.loads(x.split(c_options.separator)[1])))
    else:
        input_rdd1 = sc.sequenceFile(inputFilename1).mapValues(lambda x: json.loads(x))

    if baseFormat == "text":
        base_rdd = sc.textFile(baseFilename).map(lambda x: (x.split(c_options.separator)[0], json.loads(x.split(c_options.separator)[1])))
    else:
        base_rdd = sc.sequenceFile(baseFilename).mapValues(lambda x: json.loads(x))

    join_rdd = sc.textFile(joinResultFilename)

    result_rdd = EntityMerger.merge_rdds(input_rdd1, inputPath, base_rdd, join_rdd, removeElements)

    print "Write output to:", outputFilename
    if outputFileFormat == "text":
        result_rdd.saveAsTextFile(outputFilename)
    else:
        result_rdd.mapValues(lambda x: json.dumps(x)).saveAsSequenceFile(outputFilename)
