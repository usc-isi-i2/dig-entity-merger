#!/usr/bin/env python

from pyspark import SparkContext

from optparse import OptionParser
import json
from jsonUtil import JSONUtil
from fileUtil import FileUtil

class EntityDeduplicator:
    def __init__(self):
        pass

    def deduplicate(self, input_json, input_path):
        input_path_arr = input_path.strip().split(".")
        if len(input_path_arr) > 1:
            json_path = ".".join(input_path_arr[0: len(input_path_arr)-1])
            input_objs = JSONUtil.extract_objects_from_path(input_json, json_path)
        else:
            input_objs = JSONUtil.to_list(input_json)

        print "Got objects:", json.dumps(input_objs)

        last_path_elem = input_path_arr[len(input_path_arr)-1]
        for input_obj in input_objs:
            if last_path_elem in input_obj:
                last_input_obj = input_obj[last_path_elem]
                if isinstance(last_input_obj, list):
                    seen_objs = set()
                    for part in last_input_obj:
                        part_str = json.dumps(part)
                        if part_str in seen_objs:
                            last_input_obj.remove(part)
                        else:
                            seen_objs.add(json.dumps(part))

        return input_json


if __name__ == "__main__":
    sc = SparkContext(appName="DIG-ENTITY_DEDUPLICATOR")

    usage = "usage: %prog [options] inputDataset inputDatasetFormat inputPath " \
            "outputFilename outoutFileFormat"
    parser = OptionParser()
    parser.add_option("-r", "--separator", dest="separator", type="string",
                      help="field separator", default="\t")

    (c_options, args) = parser.parse_args()
    print "Got options:", c_options
    inputFilename = args[0]
    inputFileFormat = args[1]
    inputPath = args[2]

    print "Read ", inputFileFormat, " file from ", inputFilename, " with path:", inputPath
    outputFilename = args[3]
    outputFileFormat = args[4]

    print "Write output to:", outputFilename
    fileUtil = FileUtil(sc)
    input_rdd = fileUtil.load_json_file(inputFilename, inputFileFormat, c_options)
    result_rdd = input_rdd.mapValues(lambda x: EntityDeduplicator().deduplicate(x, inputPath))

    fileUtil.save_json_file(result_rdd, outputFilename, outputFileFormat, c_options)