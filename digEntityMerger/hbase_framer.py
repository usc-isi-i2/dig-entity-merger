from framer import *
import hashlib
from pyspark import StorageLevel
import binascii
import sys

def load_rdd_from_hbase(sc, input_tablename, input_column, zookeeper, is_json=True):
    hbase_conf = {"hbase.mapreduce.inputtable": input_tablename,
                  "hbase.mapreduce.scan.columns": input_column,
                  "hbase.zookeeper.quorum": zookeeper,
                  }
    input_rdd = sc.newAPIHadoopRDD("org.apache.hadoop.hbase.mapreduce.TableInputFormat",
                   "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
                   "org.apache.hadoop.hbase.client.Result",
                   conf=hbase_conf,
                   keyConverter="org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter",
                   valueConverter="org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter")
    def convertToPair(x):
        key = x[0]
        value = json.loads(x[1])["value"]
        if is_json is True:
            value_final = json.loads(value.decode('unicode-escape'))
        else:
            value_final = value
        return (key, value_final)
    return input_rdd.map(convertToPair)


def compute_frames_increment_from_hbase(sc, frames_rdd, frames_tablename, frames_column,
                          zookeeper, numPartitions=10):
    (frames_column_family, frames_column_name) = frames_column.split(":")

    hbase_conf = {"hbase.mapreduce.inputtable": frames_tablename,
                  "hbase.mapreduce.scan.columns": frames_column,
                  "hbase.zookeeper.quorum": zookeeper}

    #Step1 : Load the hashes of existing frames
    # def convert_key_to_unicode(key):
    #     unicode_key = re.sub('\\\\x([a-fA-F\\d][a-fA-F\\d])', lambda x: binascii.unhexlify((x.group(1))), key)
    #     return (unicode_key)

    existing_frames_rdd = sc.newAPIHadoopRDD("org.apache.hadoop.hbase.mapreduce.TableInputFormat",
                   "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
                   "org.apache.hadoop.hbase.client.Result",
                   conf=hbase_conf,
                   keyConverter="org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter",
                   valueConverter="org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter")\
                    .map(lambda x: (str(x[0]), str(json.loads(x[1])["value"])))\
                    .repartition(numPartitions) \
                    .persist(StorageLevel.MEMORY_AND_DISK)
    existing_frames_rdd.setName("existing_frames_rdd")

    has_existing_rdd = False
    for x in existing_frames_rdd.take(1):
        print "Existing frames RDD"
        has_existing_rdd = True
        print(x[0], x[1])

    #Step2: Generate the hashes of all frames that were generated
    #First generate clean hex uri keys
    frames_rdd2 = frames_rdd.map(lambda x: (x[0].encode('utf-8').encode('hex'), x[1]))\
                    .persist(StorageLevel.MEMORY_AND_DISK)
    frames_rdd2.setName("frames_rdd_keyhex")

    def sha1_hash(text):
        """return upper cased sha1 hash of the string"""
        if text:
            return hashlib.sha1(text.encode('utf-8')).hexdigest().upper()
        return ''

    frames_hashes_rdd = frames_rdd2.mapValues(lambda x: (sha1_hash(json.dumps(x, sort_keys=True)), x))\
                    .persist(StorageLevel.MEMORY_AND_DISK)
    frames_hashes_rdd.setName("frames_hashes_rdd")

    # for x in frames_hashes_rdd.take(1):
    #     print "frames RDD hashes"
    #     print(x[0], x[1])

    if has_existing_rdd is True:

        #Step3: Get the frame:hashes that are completely new
        frames_hashes_new_rdd = frames_hashes_rdd.subtractByKey(existing_frames_rdd, numPartitions)
        # for x in frames_hashes_new_rdd.take(1):
        #     print "frames_hashes_new_rdd hashes"
        #     print(x[0], x[1],type(x[1]))

        #Step4: Get the frame:hashes that also existed before
        frames_hashes_matching_exiting_rdd = frames_hashes_rdd.join(existing_frames_rdd, numPartitions)
        # for x in frames_hashes_matching_exiting_rdd.take(1):
        #     print "frames_hashes_matching_exiting_rdd hashes"
        #     print(x[0], x[1])


        def is_frame_updated(hashes):
            if hashes[0][0] != hashes[1]:
                return True
            return False

        #Step 5: For frame:hashes that existed before, check if they have changed, get the changed ones
        #The new hashes to add are the ones that have changes, or that are completely new frames
        frames_hashes_to_add = frames_hashes_matching_exiting_rdd.filter(lambda x: is_frame_updated(x[1]))\
                                    .map(lambda x: (x[0], x[1][0]))\
                                    .union(frames_hashes_new_rdd) \
                                    .persist(StorageLevel.MEMORY_AND_DISK)

        hashes_to_add = frames_hashes_to_add.mapValues(lambda x: x[0])

        # Step 7: Get and return the new/changed frames
        def convert_to_utf(x):
            return binascii.unhexlify(x).decode('utf-8')

        frames_to_add = frames_hashes_to_add.mapValues(lambda x: x[1]).map(lambda x: (convert_to_utf(x[0]), x[1]))
        # for x in frames_to_add.take(1):
        #     print "frames_to_add"
        #     print(x[0], x[1])

    else:
        hashes_to_add = frames_hashes_rdd.mapValues(lambda x: x[0])
        frames_to_add = frames_rdd

    #Step8: Save back the new/changes hashes in HBase
    def generate_hbase_row(key, value):
        result = []
        result.append(key)
        result.append(frames_column_family)
        result.append(frames_column_name)
        result.append(value)
        return result
    hashes_to_add_hbase = hashes_to_add.map(lambda x: (x[0], generate_hbase_row(x[0], x[1])))
    # for x in hashes_to_add_hbase.take(1):
    #     print "hashes_to_add to HBASE"
    #     print(x)

    write_conf = {
        "hbase.zookeeper.qourum": zookeeper,
        "hbase.mapred.outputtable": frames_tablename,
        "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
        "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
        "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"
    }
    try:
        hashes_to_add_hbase.saveAsNewAPIHadoopDataset(conf=write_conf,
                    keyConverter="org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter",
                    valueConverter="org.apache.spark.examples.pythonconverters.StringListToPutConverter")
    except:
        print "ERROR: Could not save the hashes to HBASE, continuing..."
        print sys.exc_info()[0]


    return frames_to_add

