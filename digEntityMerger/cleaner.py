#!/usr/bin/env python

from pyspark import SparkContext

from optparse import OptionParser
import json
from jsonUtil import JSONUtil
from fileUtil import FileUtil


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


class EntityCleaner:
    def __init__(self):
        pass

    @staticmethod
    def clean_rdds(inputRDD, inputPath, baseRDD):
        #1. Extract the source_uri from inputRDD
        #output: source_uri, input_uri
        input_source_rdd = inputRDD.flatMapValues(lambda x: JSONUtil.extract_values_from_path(x, inputPath)) \
            .map(lambda (x, y): (y, x))

        #2. JOIN extracted source_uri with base
        #output source_uri, (input_uri, base_json)
        merge3 = input_source_rdd.leftOuterJoin(baseRDD)

        #3. Make input_uri as the key
        #output: input_uri, (source_uri, base_json)
        merge4 = merge3.map(lambda (source_uri, join_res): (join_res[0], (source_uri, join_res[1])))

        #4. Group results by input_uri
        #output: input_uri, list(source_uri, base_json)
        merge5 = merge4.groupByKey()

        #5 Merge in input_json
        #output: input_uri, list(input_json), list(source_uri, base_json)
        merge6 = inputRDD.cogroup(merge5)

        #6 Replace JSON as necessary
        result = merge6.mapValues(lambda x: merge_json(x[0], x[1], inputPath, []))
        return result


if __name__ == "__main__":
    sc = SparkContext(appName="DIG-ENTITY_CLEANER")

    usage = "usage: %prog [options] inputDataset inputDatasetFormat inputPath" \
            "baseDataset baseDatasetFormat" \
            "outputFilename outoutFileFormat"
    parser = OptionParser()
    parser.add_option("-r", "--separator", dest="separator", type="string",
                      help="field separator", default="\t")

    (c_options, args) = parser.parse_args()
    print "Got options:", c_options
    inputFilename1 = args[0]
    inputFileFormat1 = args[1]
    inputPath = args[2]

    baseFilename = args[3]
    baseFormat = args[4]

    outputFilename = args[5]
    outputFileFormat = args[6]

    print "Write output to:", outputFilename
    fileUtil = FileUtil(sc)
    input_rdd1 = fileUtil.load_json_file(inputFilename1, inputFileFormat1, c_options)
    base_rdd = fileUtil.load_json_file(baseFilename, baseFormat, c_options)

    result_rdd = EntityCleaner.clean_rdds(input_rdd1, inputPath, base_rdd)

    print "Write output to:", outputFilename
    fileUtil.save_json_file(result_rdd, outputFilename, outputFileFormat, c_options)