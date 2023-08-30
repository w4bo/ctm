# -*- coding: utf-8 -*-

import codecs

conf = [
    ["PAMPA_breastcancer_positive", "Infinity", 1, 1, 15, 1, "Infinity", 1, "true"],
    ["PAMPA_breastcancer_positive", "Infinity", 1, 1, 12, 1, "Infinity", 1, "true"],
    ["PAMPA_synthetic_1000000_30_5", "Infinity", 1, 1, 25, 1, "Infinity", 1, "true"],
    ["PAMPA_synthetic_10000000_30_5", "Infinity", 1, 1, 27, 1, "Infinity", 1, "true"],

    ["PAMPA_breastcancer_positive", "Infinity", 1, 1, 10, 1, "Infinity", 1, "true"],
    ["PAMPA_breastcancer_positive", "Infinity", 1, 1, 9, 1, "Infinity", 1, "true"],
    ["PAMPA_breastcancer_positive", "Infinity", 1, 1, 8, 1, "Infinity", 1, "true"],
    ["PAMPA_breastcancer_positive", "Infinity", 1, 1, 7, 1, "Infinity", 1, "true"],
    ["PAMPA_breastcancer_positive", "Infinity", 1, 1, 6, 1, "Infinity", 1, "true"],
    ["PAMPA_breastcancer_positive", "Infinity", 1, 1, 5, 1, "Infinity", 1, "true"],
    ["PAMPA_breastcancer_positive", "Infinity", 1, 1, 4, 1, "Infinity", 1, "true"],
    ["PAMPA_breastcancer_positive", "Infinity", 1, 1, 3, 1, "Infinity", 1, "true"],

    ["PAMPA_synthetic_1000000_30_5", "Infinity", 1, 1, 21, 1, "Infinity", 1, "true"],
    ["PAMPA_synthetic_1000000_30_5", "Infinity", 1, 1, 20, 1, "Infinity", 1, "true"],
    ["PAMPA_synthetic_1000000_30_5", "Infinity", 1, 1, 19, 1, "Infinity", 1, "true"],
    ["PAMPA_synthetic_1000000_30_5", "Infinity", 1, 1, 18, 1, "Infinity", 1, "true"],
    ["PAMPA_synthetic_1000000_30_5", "Infinity", 1, 1, 17, 1, "Infinity", 1, "true"],

    ["PAMPA_synthetic_10000000_30_5", "Infinity", 1, 1, 25, 1, "Infinity", 1, "true"],
    ["PAMPA_synthetic_10000000_30_5", "Infinity", 1, 1, 24, 1, "Infinity", 1, "true"],
    ["PAMPA_synthetic_10000000_30_5", "Infinity", 1, 1, 23, 1, "Infinity", 1, "true"],
    ["PAMPA_synthetic_10000000_30_5", "Infinity", 1, 1, 22, 1, "Infinity", 1, "true"],
    ["PAMPA_synthetic_10000000_30_5", "Infinity", 1, 1, 21, 1, "Infinity", 1, "true"],
    ["PAMPA_synthetic_10000000_30_5", "Infinity", 1, 1, 20, 1, "Infinity", 1, "true"],

    ["geolife", "Infinity", 1, 1, 10, 5, "Infinity", 1, "true"],
    ["geolife", "Infinity", 1, 1, 10, 6, "Infinity", 1, "true"],
    ["geolife", "Infinity", 1, 1, 10, 7, "Infinity", 1, "true"],

    ["besttrj", "Infinity", 1, 1, 10, 10, "Infinity", 1, "true"],
    ["besttrj", "Infinity", 1, 1, 10, 9, "Infinity", 1, "true"],
    ["besttrj", "Infinity", 1, 1, 10, 8, "Infinity", 1, "true"],

    ["geolife", "Infinity", 100, 0.2, 10, 5, 1400, 1, "false"],
    ["geolife", "Infinity", 100, 0.2, 10, 5, 2800, 1, "false"],
    ["geolife", "Infinity", 100, 0.2, 10, 5, 4200, 1, "false"],
    ["geolife", "Infinity", 100, 0.2, 10, 5, 5600, 1, "false"],

    ["geolife", "Infinity", 100, 0.2, 10, 5, "Infinity", 1, "false"],
    ["geolife", "Infinity", 100, 0.2, 10, 5, "Infinity", 4, "false"],
    ["geolife", "Infinity", 100, 0.2, 10, 5, "Infinity", 10, "false"],

    ["besttrj", "Infinity", 100, 0.2, 10, 5, 1400, 1, "false"],
    ["besttrj", "Infinity", 100, 0.2, 10, 5, 2800, 1, "false"],
    ["besttrj", "Infinity", 100, 0.2, 10, 5, 4200, 1, "false"],
    ["besttrj", "Infinity", 100, 0.2, 10, 5, 5600, 1, "false"],

    ["besttrj", "Infinity", 1000, 0.2, 10, 5, "Infinity", 1, "false"],
    ["besttrj", "Infinity", 1000, 0.2, 10, 5, "Infinity", 4, "false"],
    ["besttrj", "Infinity", 1000, 0.2, 10, 5, "Infinity", 10, "false"],
]


def conf2str(x):
    return str(x[0]) + "_" + str(x[1]) + "_" + str(x[2]) + "_" + str(int(x[3] * 10)) + "_" + str(x[4]) + "_" + str(
        x[5]) + "_" + str(x[6]) + "_" + str(x[7])

with codecs.open("run_pampahd_tests.sh", "w", "utf-8") as f1:
    with codecs.open("run2_trajflow_tests.sh", "w", "utf-8") as f:
        for x in conf:
            conf1 = conf2str(x)
            executors = 9
            ramPerExecutor = 15000  # 21800
            cpuPerExecutor = 3
            partitions = executors * cpuPerExecutor * 3
            if "PAMPA" in x[0]:
                f1.write(
                    "spark-submit --files=/etc/hive/conf/hive-site.xml --driver-java-options \"-Dhive.metastore.uris=thrift://isi-bigcluster1.csr.unibo.it:9083\" --conf \"spark.driver.extraJavaOptions=-Dlog4jspark.root.logger=WARN,console\" --conf spark.memory.fraction=0.9 --driver-memory 5G --num-executors " + str(
                        executors) + " --executor-memory " + str(ramPerExecutor) + "M --executor-cores " + str(
                        cpuPerExecutor) + " --master yarn --deploy-mode client --class \"it.unibo.big.PampaHD\" BIG-trajectory-all.jar  " + str(
                        x[0]) + " " + str(x[4]) + " 10000000 " + str(
                        partitions) + " 2>&1 | tee logs/log_pampahd_" + str(x[0]) + "_" + str(x[4]) + ".txt\n"
                )

print("Done")