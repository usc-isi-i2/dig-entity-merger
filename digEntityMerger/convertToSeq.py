#!/usr/bin/env python

from pyspark import SparkContext

from optparse import OptionParser
from fileUtil import FileUtil

if __name__ == "__main__":
    sc = SparkContext(appName="DIG-TEXT-TO-SEQ")

    usage = "usage: %prog [options] inputDataset outputFilename"
    parser = OptionParser()
    parser.add_option("-r", "--separator", dest="separator", type="string",
                      help="field separator", default="\t")

    (c_options, args) = parser.parse_args()
    print "Got options:", c_options
    inputFilename1 = args[0]
    outputFilename = args[1]

    print "Write output to:", outputFilename
    fileUtil = FileUtil(sc)
    input_rdd = fileUtil.load_json_file(inputFilename1, "text", c_options)

    print "Write output to:", outputFilename
    fileUtil.save_json_file(input_rdd, outputFilename, "sequence", c_options)