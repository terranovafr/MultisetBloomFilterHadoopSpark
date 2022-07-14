import sys
import math
import pickle
import mmh3
from bitarray import bitarray
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


# Initialization of the spark application
def init_spark(app_name, master_config):
    conf = (SparkConf().setAppName(app_name).setMaster(master_config))
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    return (sc, spark)

# Dumps a bloom filter to file through pickle library. A bloom filter is in the form:
# (rating, k, m, bf_bytes[])
def write_bf_to_file(filter):
    with open("rate-{}.bf".format(filter[0]), "wb") as file:
        pickle.dump(filter, file)


# Rounds a rating to the closest integer
def round_vote(x):
    return int(round(float(x)))

# Applies the formula to get M, given N and FP rate
def get_M(n, p):
    return math.ceil(-((n * math.log(p)) / math.pow(math.log(2), 2)))

# Applies the formula to get K, given FP
def get_K(p):
    return math.ceil(-math.log(p))

# Given a key and a list of titles, returns a distinct set of integers 
# calculated generating k hashes for each title
def calc_hash(key, values):
    global broadcast_k, broadcast_m_values
    lst = []
    for title in values:
        for i in range(broadcast_k.value):
            lst.append(abs(mmh3.hash(title, i)) % broadcast_m_values.value[key - 1])
    return list(set(lst))


# Creates a bloom filter with the help of the bitarray library
def create_bloom_filter(key, values):
    global broadcast_k, broadcast_m_values
    bf = bitarray(broadcast_m_values.value[key - 1])
    bf.setall(0)
    for v in values:
        bf[v] = True
    return bf

# Reads a bloom filter from a file using pickle, returning four results: rating, k, m, bytes of the bf
def read_filter(filename):
    with open(filename, "rb") as file:
        object = pickle.load(file)
        return object[0], object[1], object[2], object[3]

# Check the command line arguments if driver
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: main.py <dataset> <0/1> <fp>")
        exit()


    dataset_filename = sys.argv[1]
    test_flag = int(sys.argv[2])
    fp = float(sys.argv[3])

    sc, spark = init_spark("SparkBFs", "yarn")


    rdd = spark.read.csv(dataset_filename, sep='\t', header=True).drop('numVotes').select("tconst", "averageRating").rdd

    # The dataset is now in job2 initial dataset form: [(rating1, 'title0001'), (rating2, 'title0002'), ...]
    final_dataset = rdd.map(lambda x: (round_vote(x[1]), x[0]))

    count_entries_dataset = rdd.map(lambda x: (round_vote(x[1]), 1))

    # Counts the number of titles for each rating in the dataset
    vote_counts = count_entries_dataset.reduceByKey(lambda x, y: x + y).sortByKey()

    # Collects an array of the M values, in order of rating 
    m_values = vote_counts.map(lambda x: (x[0], get_M(x[1], fp))).sortByKey().values().collect()

    # Collects an array of the N values, in order of rating
    n_values = vote_counts.values().collect()

    # Calculate k
    k = get_K(fp)

    # Broadcasts the M values and K to all workers for future use
    broadcast_m_values = sc.broadcast(m_values)
    broadcast_k = sc.broadcast(k)

    # Groups the final_dataset by key, each group will be needed for a single bloom filter
    rating_titles = final_dataset.groupByKey()

    # Puts the dataset in the form of [(key1, hash_list1[]), (key2, hash_list2[]), ...]
    hashed_dataset = rating_titles.map(lambda x: (x[0], calc_hash(x[0], x[1])))

    # Creates the bloom filters, given each key and its list of hashes
    bf_rdd = hashed_dataset.map(lambda x: (x[0], create_bloom_filter(x[0], x[1])))

    # Converts the bloom filters rdd in a writable-friendly fashion
    to_save = bf_rdd.sortByKey().map(lambda x: (x[0], broadcast_k.value, broadcast_m_values.value[x[0] - 1], x[1].tobytes())).collect()


    # Write each bloom filter to its own file
    for it in range(len(to_save)):
        write_bf_to_file(to_save[it])



    # Test the bloom filters:
    # For each bloom filter: 
    # Tests the bloom filter with a number of tests equal to its N. The titles for the test are taken from the 
    # original dataset, discarding the titles that we already know belong to the bloom filter class. If all the positions
    # of the hashes for a certain title are set in the bf, the counter for the false positives is incremented. At the 
    # end the ratio of the false positives over the number of tests is printed out to command line
    if test_flag == 1:
        test_dataset = final_dataset.collect()

        for i in range(10):
            read_rate, read_k, read_m, bytes_filter = read_filter("rate-{}.bf".format(i + 1))
            realfilter = bitarray()
            realfilter.frombytes(bytes_filter)

            count = 0
            limit = n_values[i]
            j = 0
            while limit != 0:
                fp = True
                if test_dataset[j][0] == (i + 1):
                    j = j + 1
                    continue

                for z in range(read_k):
                    hash = abs(mmh3.hash(test_dataset[j][1], z)) % read_m
                    fp = fp and realfilter[hash]

                if fp:
                    count = count + 1
                limit = limit - 1
                j = j + 1

            print((i + 1))
            print(count / n_values[i])
