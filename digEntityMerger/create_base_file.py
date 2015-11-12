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
    def load_input(x, sep):
        parts = x.split(sep)
        if len(parts) >= 2:
            uri = parts[0]
            name = parts[1]
            return uri, {"uri":uri, "name":name}
        else:
            print "\n\n****************** Got non parse line:", x
    input_rdd =sc.textFile(inputFilename1).map(lambda x: load_input(x, c_options.separator)).filter(lambda x: x != None)

    print "Write output to:", outputFilename
    fileUtil.save_json_file(input_rdd, outputFilename, "text", c_options)


