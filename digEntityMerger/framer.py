#!/usr/bin/env python

from pyspark import SparkContext
from optparse import OptionParser
import json
from jsonUtil import JSONUtil
from fileUtil import FileUtil
from basicMerger import EntityMerger

def frame_json(frame, type_to_rdd):
	document_type = frame["@type"]
	output_rdd = type_to_rdd[document_type]["rdd"]
	if len(frame.items()) > 1 :
		if "@explicit" in frame and frame["@explicit"] == True:
			output_rdd = output_rdd.map(lambda (uri, json): (uri, JSONUtil.frame_include_only_values(json, frame)))
		for key, val in frame.items():
			if key[0] == "@":
				continue
			if isinstance(val, dict) and not "@type" in val:
				continue
			if isinstance(val, dict) and "@embed" in val and val["@embed"] == False:
				continue
			# should this be every value?
			child_rdd = frame_json(val, type_to_rdd)
			output_rdd = EntityMerger.merge_rdds(output_rdd, key, child_rdd, 10)
	return output_rdd

	#recurse through the frame json document and insert the rdds just loaded by type
		#input is a json document
		#output is an rdd
	#depth first search the frame document
		# if no none @type/input-rdd properties, set output-rdd as input-rdd, return output=rdd
		# if properties to merge, 
		# for each, 
			#recursively call,
			# then pass in result to merge-rdds along with input-rdd or output-rdd if defined, 
			#set output-rdd as result from merge
		# return output

if __name__ == "__main__":
	sc = SparkContext(appName="DIG-FRAMER")
	parser = OptionParser()
	parser.add_option("-r", "--separator", dest="separator", type="string", help="field separator", default="\t")
	parser.add_option("-n", "--numPartitions", dest="numPartitions", type="int", help="number of partitions", default=5)

	(c_options, args) = parser.parse_args()
	frameFilename = args[0]
	rddFilename = args[1]
	outputFilename = args[2]
	if len(args) > 3:
		outputFileFormat = args[3]
	else :
		outputFileFormat = "text"
	type_to_rdd_json_input = open(rddFilename) 
	type_to_rdd_json = json.load(type_to_rdd_json_input)
	type_to_rdd_json_input.close()
	frame_input = open(frameFilename) 
	frame = json.load(frame_input)
	frame_input.close()
	fileUtil = FileUtil(sc)
	for key,val in type_to_rdd_json.items():
		val["rdd"] = fileUtil.load_json_file(val["path"], val["format"], c_options)
	output_rdd = frame_json(frame, type_to_rdd_json)
	print "Write output to:", outputFilename
	fileUtil.save_json_file(output_rdd, outputFilename, outputFileFormat, c_options)
