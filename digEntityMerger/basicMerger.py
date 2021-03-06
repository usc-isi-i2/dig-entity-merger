#!/usr/bin/env python

from pyspark import SparkContext

from optparse import OptionParser
import json
from digEntityMerger.jsonUtil import JSONUtil
from digEntityMerger.fileUtil import FileUtil


#https://issues.apache.org/jira/browse/SPARK-4851
#Am not able to make it a static method inside EntityMerger
def merge_json(input_jsons, merge_uri_and_jsons, input_path):
    # print ("INPUT JSONS:", input_jsons)
    for x in input_jsons:
        input_json = x
        break


    findReplaceMap = dict()
    for merge_uri_and_json in merge_uri_and_jsons:
        # print ("merge_uri_and_json:", merge_uri_and_json)
        # print ("==========")
        uri = merge_uri_and_json[0]
        # print("uri:", uri)
        json_obj = merge_uri_and_json[1]
        findReplaceMap[uri] = json_obj


    # print ("INPUT JSON:", input_json)
    JSONUtil.replace_values_at_path_batch(input_json, input_path, findReplaceMap, [])
    # print ("OUTPUT JSON:", input_json)

    return input_json


def load_linking_row(json_str, uri_as_key):
    json_obj = json.loads(json_str)

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
    def merge_rdds(inputRDD, inputPath, baseRDD, numPartitions, maxNumMerge=None):
        # inputRDD: input_uri, input_json
        # base_rdd: merge_uri, base_json
        #
        # inputPath: path in input_json where merge_uri will be found
        #

        #2. Extract the source_uri from inputRDD
        #output: merge_uri, input_uri
        if maxNumMerge is not None:
            input_source_rdd = inputRDD.flatMapValues(lambda x: JSONUtil.extract_values_from_path(x, inputPath)[0:maxNumMerge]) \
                .map(lambda x: (x[1], x[0]))
        else:
            input_source_rdd = inputRDD.flatMapValues(lambda x: JSONUtil.extract_values_from_path(x, inputPath)) \
                .map(lambda x: (x[1], x[0]))

        # print("merge_rdds at path", inputPath)
        # for x in input_source_rdd.take(1):
        #     print("input_source_rdd:", x)
        #
        # for x in baseRDD.take(1):
        #     print("baseRDD:", x)

        if numPartitions > 0:
            input_source_rdd = input_source_rdd.partitionBy(numPartitions)

            #3. JOIN extracted source_uri with base
            #output merge_uri, (input_uri, base_json)
            merge3 = input_source_rdd.join(baseRDD, numPartitions)
        else:
            merge3 = input_source_rdd.join(baseRDD)

        # for x in merge3.take(1):
        #     print("merge3:", x)

        #4. Make input_uri as the key
        #output: input_uri, (merge_uri, base_json)
        merge4 = merge3.map(lambda x: (x[1][0], (x[0], x[1][1]))).partitionBy(numPartitions)

        # for x in merge4.take(1):
        #     print("merge4:", x)

        #5. Group results by input_uri
        #output: input_uri, list(merge_uri, base_json))
        if numPartitions > 0:
            
            #6 Merge in input_json
            #output: input_uri, list(input_json), list(merge_uri, base_json)
            merge6 = inputRDD.cogroup(merge4, numPartitions)
        else:

            #6 Merge in input_json
            #output: input_uri, list(input_json), list(merge_uri, base_json)
            merge6 = inputRDD.cogroup(merge4)

        #7 Replace JSON as necessary
        # for x in merge6.take(1):
        #     print("merge6:", x[0], list(x[1][0]), list(x[1][1]))

        result = merge6.mapValues(lambda x: merge_json(x[0], x[1], inputPath))

        # for x in result.take(1):
        #     print("result:", x)
        return result


if __name__ == "__main__":
    sc = SparkContext(appName="DIG-ENTITY_MERGER")

    usage = "usage: %prog [options] inputDataset inputDatasetFormat" \
            "baseDataset baseDatasetFormat" \
            "outputFilename outoutFileFormat inputPath"
    parser = OptionParser()
    parser.add_option("-r", "--separator", dest="separator", type="string",
                      help="field separator", default="\t")
    parser.add_option("-n", "--numPartitions", dest="numPartitions", type="int",
                      help="number of partitions", default=5)

    (c_options, args) = parser.parse_args()
    inputFilename1 = args[0]
    inputFileFormat1 = args[1]
    inputPath = args[2]

    baseFilename = args[3]
    baseFormat = args[4]

    outputFilename = args[5]
    outputFileFormat = args[6]

    print("Got options:", c_options, ", " \
                         "input:", inputFilename1, ",", inputFileFormat1, ",", inputPath, \
                         ", base:", baseFilename, ",", baseFormat)

    fileUtil = FileUtil(sc)
    input_rdd1 = fileUtil.load_json_file(inputFilename1, inputFileFormat1, c_options)
    base_rdd = fileUtil.load_json_file(baseFilename, baseFormat, c_options)

    result_rdd = EntityMerger.merge_rdds(input_rdd1, inputPath, base_rdd, c_options.numPartitions)

    print("Write output to:", outputFilename)
    for x in result_rdd.collect():
        print("---------------------")
        print(x)
    fileUtil.save_json_file(result_rdd, outputFilename, outputFileFormat, c_options)