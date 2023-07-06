# MultisetBloomFilterHadoopSpark
In this project, we proposed a solution for the **multiple-set** matching problem, defining a multi-set **Bloom** **Filter**.<br>
A multi-set Bloom Filter is a space-efficient probabilistic data structure that allows to check the membership of an element in multiple sets.<br>
The new data structure presented in this project has associated multiple sets of data and supports the construction operation of all the sets through an efficient construction operation.<br>
The IMDb dataset containing ratings of movies has been used as a reference for building and assessing the performances of the Bloom Filter.<br>
An implementation based on the **MapReduce** paradigm is presented, specifically employing the **Hadoop** and **Spark** frameworks.<br>

## Project Structure
The project is divided into two main modules:
- BloomFilterHadoop: containing the implementation of the multi-set Bloom Filter in Java using the Hadoop Framework
- BloomFilterSpark: containing the implementation of the multi-set Bloom Filter in Python using the Spark Framework

## Hadoop
The Bloom Filter construction can be decomposed in two stages, suitable for a MapReduce implementation:
1. Parameter Calculation Stage, where the parameters of each Bloom Filter {mi, ki} are computed, based on the number of elements with a given rating in the dataset.
![image](https://github.com/terranovaa/MultisetBloomFilterHadoopSpark/assets/61695945/9a53f3ca-f894-4150-bbc4-6424fdf8ecc8)
![image](https://github.com/terranovaa/MultisetBloomFilterHadoopSpark/assets/61695945/997ba822-db11-442f-9a76-506849763e41)

2. Construction Stage, where the Bloom Filters are filled based on the results of the hash functions.
![image](https://github.com/terranovaa/MultisetBloomFilterHadoopSpark/assets/61695945/7a69b458-b37b-425d-89fa-5b20115837db)
![image](https://github.com/terranovaa/MultisetBloomFilterHadoopSpark/assets/61695945/5b2715cd-2acf-419a-a478-dcd177bf36fb)

![image](https://github.com/terranovaa/MultisetBloomFilterHadoopSpark/assets/61695945/666b5245-d5b8-45de-acbe-e11ca26d5e53)

## Spark
Starting from the solutions highlighted in the Hadoop Design, the following lineage graphs will show the computation workflow of the Spark Framework
![image](https://github.com/terranovaa/MultisetBloomFilterHadoopSpark/assets/61695945/285922f2-8cca-4485-8333-3c1eff50f407)
![image](https://github.com/terranovaa/MultisetBloomFilterHadoopSpark/assets/61695945/d933e9c5-bd3c-4fd6-ad29-32d16311dd38)



## Complexity
Our solution has a space complexity of 𝑂(𝑚∗𝑙) where 𝑚 is the maximum size among all Bloom Filters and 𝑙 is the total number of possible ratings.<br>
Moreover, the operation of insertion has a complexity of O(𝑘∗𝑛) where 𝑘 is the maximum number of hash functions among all Bloom Filters and 𝑛 is the total number of elements.


