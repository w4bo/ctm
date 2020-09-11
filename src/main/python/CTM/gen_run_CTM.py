# -*- coding: utf-8 -*-
# Generate configuration for the test runs. In each run, one parameters ranges in all values while the remaining parameters are given the default value.
import codecs
import os

filename = "../../../../run_CTM.sh"
datasets = ["trajectory.milan_standard", "trajectory.geolife_standard"]  # "trajectory.oldenburg_standard",
params = {
    "minsize": {
        "trajectory.geolife_standard": {"values": ["100", "250", "500"], "default": "100"},
        "trajectory.milan_standard":   {"values": ["400", "600", "800"], "default": "400"},
    },
    "minsup":       {"values": ["4", "5", "6", "7"], "default": "5"},
    "bins": {
        "trajectory.geolife_standard": {"values": ["8"], "default": "8"},      # cell size (125m*bins, 125m*bins)
        "trajectory.milan_standard":   {"values": ["15"], "default": "15"},
    },
    "epss":         {"values": ["1", "3", "5", "Infinity"], "default": "3"},
    "timescale":    {"values": ["daily", "notime"], "default": "daily"},  # ,"absolute", "weekly"
    "unitt":        {"values": ["3600"], "default": "3600"},
    "bint":         {"values": ["6"], "default": "6"},
    "epst":         {"values": ["1"], "default": "1"},
    "nexecutors":   {"values": ["10"], "default": "10"},
    "ncores":       {"values": ["3"], "default": "3"},
    "maxram":       {"values": ["20g"], "default": "20g"},
    "repfreq":      {"values": ["1"], "default": "1"},
    "storagethr":   {"values": ["0"], "default": "0"},
    "limit": {
        "trajectory.geolife_standard": {"values": ["5000", "10000", "15000"], "default": "15000"},
        "trajectory.milan_standard":   {"values": ["100000", "1000000", "10000000"], "default": "10000000"},
    },
}

# While testing `key`, do not run configurations including `values`
exclude = {
    "bint": ["notime"],
    "epst": ["notime"]
}

def giveExecutionPermissionToFile(path):
    st = os.stat(path)
    os.chmod(path, st.st_mode | 0o111)

runs = []
with codecs.open(filename, "w", "utf-8") as w:
    for dataset in datasets:
        for key, value in params.items():
            values = value["values"] if not dataset in value else value[dataset]["values"]
            if len(values) == 1: # useless to iterate on parameters with a single value, these are already tested
                continue
            for v in values:
                s = " --tbl=" + dataset + (" --euclidean" if "oldenburg" in dataset else "")
                s += " --{key}={value}".format(key=key, value=v)
                to_exclude = False
                for ikey, ivalue in params.items():
                    default = ivalue["default"] if not dataset in ivalue else ivalue[dataset]["default"]
                    if key in exclude and default in exclude[key]: # do not generate excluded configurations
                        to_exclude = True
                        continue
                    if key != ikey:
                        s += " --{key}={value}".format(key=ikey, value=default)
                print(s)
                # See https://docs.oracle.com/javase/8/docs/technotes/guides/rmi/javarmiproperties.html
                # spark.driver.extraJavaOptions
                # -Djava.rmi.server.hostname=isi-bigcluster8
                s = "spark-submit --conf \"spark.driver.extraJavaOptions=-Dlog4jspark.root.logger=WARN,console\" " \
                                    "--conf \"spark.executor.extraJavaOptions=-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=8090 " \
                                            "-Dcom.sun.management.jmxremote.rmi.port=8091 -Dcom.sun.management.jmxremote.authenticate=false " \
                                            "-Dcom.sun.management.jmxremote.ssl=false \" " \
                                    "--conf spark.memory.fraction=0.8 " \
                                    "--conf spark.driver.maxResultSize=10g --driver-memory 20G " \
                                    "--master yarn --deploy-mode client " \
                                    "--class it.unibo.big.CTM build/libs/CTM-all.jar" + s
                if not to_exclude:
                    runs.append(s)
    w.write('\n'.join(runs))

giveExecutionPermissionToFile(filename)
print("Done. Nruns: " + str(len(runs)))
