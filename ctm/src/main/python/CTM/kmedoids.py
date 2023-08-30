import csv
import numpy as np
import random
import time
from pyclustering.cluster.kmedoids import kmedoids;

# -----------------------------------------------------------------------------
# Parameters
infiles = ['1000traces', 'traces']
# infiles = ['1000traces']
ks = [5, 10, 20]
gridsizes = [2, 3]


# -----------------------------------------------------------------------------

def run(infile, k, gridsize, rowlim=2000):
    data = []
    ids = []

    random.seed(2019)

    def rnd(x):
        return round(float(x), gridsize)

    with open(infile + '.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=';')
        for rownum, row in enumerate(csv_reader):
            data.append([str(rnd(x.split("_")[0])) + "_" + str(rnd(x.split("_")[1])) for x in row[2].split(",")])
            ids.append(row[0] + "_" + row[1])
            if (rownum == rowlim):
                break

    def our_distance(data):
        rows = []
        for x in data:
            row = []
            x = set(x)
            for y in data:
                y = set(y)
                row.append(1 - 2 * len(x & y) / (len(x) + len(y)))
            rows.append(row)
        return np.array(rows)

    print("Estimating distances...", end='')
    start_time = time.time()
    D = our_distance(data)
    elapsed_time = time.time() - start_time
    print(" done in " + time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))

    initial_medoids = [random.randint(1, len(D)) - 1 for r in range(k)]
    kmedoids_instance = kmedoids(D, initial_index_medoids=initial_medoids, data_type='distance_matrix', ccore=False,
                                 tolerance=0.001)
    # kmedoids_instance = kmedoids([[0,0], [12,1], [1,1], [1,12]], initial_index_medoids=[0, 1])

    print("Clustering...")
    start_time = time.time()
    kmedoids_instance.process()
    elapsed_time = time.time() - start_time
    print("Done in " + time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))

    clusters = kmedoids_instance.get_clusters()
    medoids = kmedoids_instance.get_medoids()

    with open("tableau_" + infile + "_rows-" + str(rowlim) + "_k-" + str(k) + "_gridsize-" + str(gridsize) + ".csv",
              mode='w') as file:
        writer = csv.writer(file, delimiter=';', lineterminator='\n')
        writer.writerow(
            ["trajid", "latitude", "longitude", "pointid", "clusterid", "medoid", "clusterdist", "clustersize"])
        label = 0
        for cluster in clusters:  # this is a cluster
            dist = 0
            medoid = list(set(medoids) & set(cluster))
            if len(medoid) > 1:
                print("too many medoids in this cluster")
                medoid = medoid[0]
            for tupleid in cluster:  # trajectory is a cluster
                dist = dist + D[medoid, tupleid][0]
            for tupleid in cluster:  # trajectory is a cluster
                for p, x in enumerate(data[tupleid]):  # point in trajectory
                    writer.writerow(
                        [ids[tupleid], x.split("_")[0], x.split("_")[1], p, label, 1 if tupleid in medoids else 0, dist,
                         len(cluster)])
            label = label + 1


for infile in infiles:
    for k in ks:
        for gridsize in gridsizes:
            print("\n--- Run. file: " + infile + ".csv, k: " + str(k) + ", gridsize: " + str(gridsize))
            run(infile, k, gridsize)
