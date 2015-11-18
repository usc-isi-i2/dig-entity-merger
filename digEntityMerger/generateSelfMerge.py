#!/usr/bin/env python

from pyspark import SparkContext

from optparse import OptionParser
from fileUtil import FileUtil
import json

if __name__ == "__main__":
    sc = SparkContext(appName="DIG-ENTITY_MERGER")

    usage = "usage: %prog [options] inputDataset inputDatasetFormat" \
            "outputFilename"
    parser = OptionParser()
    parser.add_option("-r", "--separator", dest="separator", type="string",
                      help="field separator", default="\t")

    (c_options, args) = parser.parse_args()
    inputFilename1 = args[0]
    inputFileFormat1 = args[1]
    outputFilename = args[2]

    print "Got options:", c_options, ",input:", inputFilename1 + ", output:", outputFilename

    fileUtil = FileUtil(sc)
    input_rdd1 = fileUtil.load_json_file(inputFilename1, inputFileFormat1, c_options)

    def write_result(x):
        key = x[0]
        #print "Got key:", key
        return json.dumps({"uri":key, "matches":[{"uri": key}]})

    result = input_rdd1.map(write_result)
    result.saveAsTextFile(outputFilename)
