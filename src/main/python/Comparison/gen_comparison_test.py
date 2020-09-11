# -*- coding: utf-8 -*-
# This script will generate a comparison between cute and spare dataset.
# For this script to work, must be inside a folder with four jar:
#   BIG-trajectory-similarity.jar
#   BIG-trajectory-writer.jar
#   Trajectory-Instrumentally-project-0.0.1-clustering-spare.jar
#   BIG-trajectory-temporal-macho.jar

# The test will be created in this way:
# For each dataset parameter configuration, it will be created a specific dataset, both as table and as TSV.
# Multiple configuration of parameters for the CUTE and SPARE bounds will be executed on each of this dataset.
# for each common parameter configuration between cute and spare (m, k, g), will be run the similarity test between each couple of results.

import codecs
import os
from os import path as pt

def mkdir(path):
    if not pt.exists(path):
        try:
            os.mkdir(path)
        except OSError:
            print("Creation of the directory %s failed" % path)

def giveExecutionPermissionToFile(path):
    st = os.stat(path)
    os.chmod(path, st.st_mode | 0o111)

def createSimilarityCommand(tsv_path, table_name):
    return "spark-submit ./BIG-trajectory-similarity.jar --input_table_name=" + table_name + " --input_tsv_path="+ tsv_path

def createCommonDataset(inputTableName, outputTableName, outputTSVPath, samplingRatio, limit):
    print(outputTSVPath)
    return "spark-submit ./BIG-trajectory-writer.jar --time_sampling_ratio=" + samplingRatio + " --input_table_name=" + inputTableName + " --output_table_name=" + outputTableName + " --output_tsv_path=" + outputTSVPath + " --limit="+str(limit)

def createGCMPLaunchCommand(input_path, output_path, m, k, l, g, epsilon, minpts):
    return "spark-submit ./Trajectory-Instrumentally-project-0.0.1-clustering-spare.jar " \
           "--input_dir="+input_path+" --output_dir="+output_path+" --gcmp_m="+m+" --gcmp_k="+k+" --gcmp_l="+str(l)+" --gcmp_g="+g+" " \
                                                                                                                              "--epsilon="+str(epsilon)+ \
           " --min_points="+str(minpts)+" --input_partitions=486 --numexecutors="+str(nexecutors)+" --numcores="+str(maxcores)+" --executormemory="+str(maxram)+" --debug=OFF --earth=0 "

def createCUTELaunchCommandWithLimit(input_table,cell_side, time_bucket_unit ,minsize, minsupp, epsilon ,timebucketthreshold, euclidean, limit):
    euclideanValue = getEuclidean(euclidean)
    if (euclideanValue != ""):
        euclideanValue = " --" + euclideanValue
    return "spark-submit --files=/etc/hive/conf/hive-site.xml --driver-java-options \"-Dhive.metastore.uris=thrift://isi-bigcluster1.csr.unibo.it:9083\" --conf \"spark.driver.extraJavaOptions=-Dlog4jspark.root.logger=WARN,console\" --conf spark.memory.fraction=0.9 --conf spark.driver.maxResultSize=5g --driver-memory 5G --master yarn --deploy-mode client ../../../../build/libs/BIG-trajectory-temporal-macho.jar --tbl=" + input_table + " --limit="+limit+" --minsize=" + minsize + " --mincoh=0 --minsup=" + minsupp + " --cellround=" + cell_side + " --radius="+epsilon+" --repartitionfrequency=1 --storagethreshold=1000000 --timebucketsize=1 --timebucketthreshold=" + timebucketthreshold + " --timebucketunit=" + time_bucket_unit + " --nexecutors=" + str(
        nexecutors) + " --ncores=" + str(
        maxcores) + " --maxram=" + maxram + " --disablepruning --weeklyscale=absolute " + euclideanValue

def createCUTELaunchCommand(input_table, output_table,cell_side, time_bucket_unit ,minsize, minsupp, timebucketthreshold, euclidean):
    euclideanValue = getEuclidean(euclidean)
    if(euclideanValue != ""):
        euclideanValue = " --" + euclideanValue
    return "spark-submit --files=/etc/hive/conf/hive-site.xml --driver-java-options \"-Dhive.metastore.uris=thrift://isi-bigcluster1.csr.unibo.it:9083\" --conf \"spark.driver.extraJavaOptions=-Dlog4jspark.root.logger=WARN,console\" --conf spark.memory.fraction=0.9 --conf spark.driver.maxResultSize=5g --driver-memory 5G --master yarn --deploy-mode client ./BIG-trajectory-temporal-macho.jar --tbl="+input_table+" --limit=Infinity --minsize="+minsize+" --mincoh=0 --minsup="+minsupp+" --cellround="+cell_side+" --radius=Infinity --repartitionfrequency=1 --storagethreshold=0 --timebucketsize=1 --timebucketthreshold="+timebucketthreshold+" --timebucketunit="+time_bucket_unit+" --nexecutors="+str(nexecutors)+" --ncores="+str(maxcores)+" --maxram="+maxram+" --disablepruning --weeklyscale=absolute --output_table_name=" + output_table + euclideanValue

def convertDatasetName(dataset):
    if("geolife_bejin" in dataset):
        return dataset.replace("geolife_bejin", "GEOLIFE_BEJIN")
    if("trajectoryflow_besttrj" in dataset):
        return dataset.replace("trajectoryflow_besttrj", "MILAN")
    return dataset

def getEuclidean(flag):
    if(flag == "true"):
        return "euclidean"
    else:
        return ""

limitArray = [52000]#[52000, 53000]
possibleDatasets = ["final_gcmp_cute_dataset"]#["tmp_geolife_bejin_sampled_600_sec_72_range"]#["geolife", "besttrj"]
epsilon_dataset = ["2000"]#["1000", "2000"]#["1230", "6150"]#["123", "615", "1230", "6150"]
min_points_dataset = ["5"]#["2", "5"]#["2", "5"]#["2", "5", "10", "20"]
possible_radius_dataset = ["750","1250", "1500", "1750", "1000", "2000", "4000"]#["1000", "2000"]#["1230000", "6150000"]#["1", "5", "10", "50"]
l = 1

possibleM = ["3", "4", "5"]#["5", "10", "15"]#["10", "20"]#["2", "3", "4", "5"]
possibleK = ["4", "5", "10"]#["5", "10", "15"]#["10", "20"]#["2", "3", "4", "5"]
possibleG = ["2", "5", "10", "1000"]#["2", "5", "1000"]#["2","5", "10","1000"]#["1", "2", "5", "10", "20", "50", "1000"]

possibleEuclideanValue = ["true"]

stringsplitter="splitter"

nexecutors = 9
maxcores = 3
maxram = "12g"

distinctsamplingsize = ["5"]#["5", "10", "15"]#["600"]#["1", "30", "60", "300", "600", "3600"]

general_hdfs_path = "/user/fnaldini/comparisontest/"

general_hdfs_dataset_path = "dataset"

general_hdfs_cluster_path = "cluster"

mkdirCmd = "hdfs dfs -mkdir "

#Step 1: Datasets creation and sampling.

with codecs.open("run_all_comparison_tests.sh", "w", "utf-8") as r:
    r.write("mkdir comparison\n")
    r.write(mkdirCmd + general_hdfs_path + "\n")
    for dataset in possibleDatasets:
        for samplingsize in distinctsamplingsize:
            for limit in limitArray:
                output_table_name = "test_" + dataset + "_" + samplingsize + "_" + str(limit - 50000)
                output_table_name = convertDatasetName(output_table_name)
                print(output_table_name)
                testName = output_table_name + "/"
                inputTSVName = general_hdfs_path + testName + general_hdfs_dataset_path
                inputTSVCommand = mkdirCmd + inputTSVName
                r.write(mkdirCmd + general_hdfs_path + testName + "\n")
                r.write(inputTSVCommand + "\n")
                r.write(mkdirCmd + general_hdfs_path + testName + general_hdfs_cluster_path + "\n")
                for m in possibleM:
                    for k in possibleK:
                        for g in possibleG:
                            for epsilon in epsilon_dataset:
                                for min_points in min_points_dataset:
                                    outputTestNamePath = "/cluster_" + m + "_" + k + "_" + g + "_"+epsilon+"_"+min_points+"/"
                                    r.write(mkdirCmd + general_hdfs_path + testName + general_hdfs_cluster_path + outputTestNamePath + "\n")
                convertDatasetCommand = createCommonDataset(dataset, output_table_name, inputTSVName, samplingsize, limit)
                r.write(convertDatasetCommand + "\n")
                for m in possibleM:
                    for k in possibleK:
                        for g in possibleG:
                            output_table_dataset = []
                            output_tsv_path = []
                            #Generating cute command launch
                            for radius in possible_radius_dataset:
                                for euclidean in possibleEuclideanValue:
                                    euclideanMetric = getEuclidean(euclidean)
                                    cuteTableName = "tmp_"+ output_table_name
                                    print(cuteTableName)
                                    outputTestName = "test_" + dataset + "_" + samplingsize + stringsplitter +"_" + m + "_" + k + "_" + g + "_" + radius +"_"+ str(limit-50000) +"_" + euclideanMetric
                                    cuteCommand = createCUTELaunchCommand(cuteTableName, outputTestName, radius, samplingsize, m, k, g, euclidean)
                                    r.write(cuteCommand + "\n")
                                    output_table_dataset.append(outputTestName)
                            for epsilon in epsilon_dataset:
                                for min_points in min_points_dataset:
                                    outputTSVSuffix = "/cluster_" + m + "_" + k + "_" + g + "_" + stringsplitter + epsilon + "_" + min_points + "/"
                                    outputTSVPath = general_hdfs_path + testName + general_hdfs_cluster_path + outputTSVSuffix
                                gcmpCommand = createGCMPLaunchCommand(inputTSVName, outputTSVPath, m, k, l, g, epsilon, min_points)
                                r.write(gcmpCommand + "\n")
                                output_tsv_path.append(outputTSVPath)
                            for table in output_table_dataset:
                                for tsv in output_tsv_path:
                                    similarityCommand = createSimilarityCommand(tsv, table)
                                    r.write(similarityCommand + "\n")

giveExecutionPermissionToFile("run_all_comparison_tests.sh")

with codecs.open("run_oldenburg_tests.sh", "w", "utf-8") as oldenburgFile:
    limitArray = ["1000000", "500000", "50000"]
    oldenburgDataset = "final_oldenburg_dataset"
    for limit in limitArray:
        command = createCUTELaunchCommandWithLimit(oldenburgDataset, "1000", "10", "100", "10", "2", "1", "true", limit)
        oldenburgFile.write(command + "\n")

giveExecutionPermissionToFile("run_oldenburg__tests.sh")
print("Done")

