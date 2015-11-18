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


def load_linking_row(tuple, uri_as_key):
    json_obj = tuple[1]
    if uri_as_key:
        key = json_obj["uri"]
        value = json_obj["matches"][0]["uri"]
    else:
        key = json_obj["matches"][0]["uri"]
        value = json_obj["uri"]
    if key != value:
        return key, value



class EntityMerger:
    def __init__(self):
        pass

    @staticmethod
    def merge_rdds(inputRDD, inputPath, baseRDD, joinRDD, removeElements, numPartitions):
        #1. Merge baseRDD and joinRDD
        #baseRDD: base_uri base_json
        #joinRDD: source_uri base_uri
        #output:  source_uri, base_json
        join_rdd_on_base = joinRDD.map(lambda x: load_linking_row(x, False)) #Get back base_uri -> source_uri
        base_merge = join_rdd_on_base.join(baseRDD).map(lambda x: (x[1][0], x[1][1]))

        #2. Extract the source_uri from inputRDD
        #output: source_uri, input_uri
        input_source_rdd = inputRDD.flatMapValues(lambda x: JSONUtil.extract_values_from_path(x, inputPath)) \
            .map(lambda (x, y): (y, x))

        #3. JOIN extracted source_uri with base
        #output source_uri, (input_uri, base_json)
        merge3 = input_source_rdd.join(base_merge, numPartitions)

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
    sc = SparkContext(appName="DIG-ENTITY_MERGER")

    usage = "usage: %prog [options] inputDataset inputDatasetFormat inputPath" \
            "baseDataset baseDatasetFormat" \
            "joinResult outputFilename outoutFileFormat commaSepRemoveAttributes"
    parser = OptionParser()
    parser.add_option("-r", "--separator", dest="separator", type="string",
                      help="field separator", default="\t")
    parser.add_option("-n", "--numPartitions", dest="numPartitions", type="int",
                      help="number of partitions", default=5)
    parser.add_option("-t", "--remove", dest="remove", type="string",
                      help="comma sep attributes to remove", default='')

    (c_options, args) = parser.parse_args()
    print "Got options:", c_options
    inputFilename1 = args[0]
    inputFileFormat1 = args[1]
    inputPath = args[2]

    baseFilename = args[3]
    baseFormat = args[4]

    joinResultFilename = args[5]
    joinFormat = args[6]

    outputFilename = args[7]
    outputFileFormat = args[8]

    removeElementsStr = c_options.remove
    removeElements = []
    if len(removeElementsStr) > 0:
        removeElements = removeElementsStr.split(",")

    print "Got options:", c_options, ", " \
                         "input:", inputFilename1, ",", inputFileFormat1, ",", inputPath, \
                         ", base:", baseFilename, ",", baseFormat, ", join:", joinResultFilename

    print "Write output to:", outputFilename
    fileUtil = FileUtil(sc)
    input_rdd1 = fileUtil.load_json_file(inputFilename1, inputFileFormat1, c_options)
    base_rdd = fileUtil.load_json_file(baseFilename, baseFormat, c_options)
    join_rdd = fileUtil.load_json_file(joinResultFilename, joinFormat, c_options)

    result_rdd = EntityMerger.merge_rdds(input_rdd1, inputPath, base_rdd, join_rdd, removeElements, c_options.numPartitions)

    fileUtil.save_json_file(result_rdd, outputFilename, outputFileFormat, c_options)