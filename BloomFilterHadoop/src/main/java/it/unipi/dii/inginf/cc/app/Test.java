package it.unipi.dii.inginf.cc.app;

import it.unipi.dii.inginf.cc.model.BloomFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.hash.MurmurHash;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class Test {
    private static final int CLASS_NUMBER = 10;
    public static void main(String args[]) throws IOException{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: BFTester <filtersFolder> <dataset>");
            System.exit(1);
        }

        BloomFilter[] filtersArray = retrieveBloomFilters(args[0], fs, conf);

        int[] entriesPerClass = getEntriesPerClass(args[1], fs);
        ArrayList<String>[] samples = fillSamples(args[1], fs);


        for (int testedFilter = 0; testedFilter < CLASS_NUMBER; testedFilter++) {
            testFilter(filtersArray[testedFilter], testedFilter, samples, entriesPerClass);
        }
    }

    private static void testFilter(BloomFilter testedFilter, int key, ArrayList<String>[] samples, int[] entriesPerClass) {
        int m = testedFilter.getM().get();
        int numTests = entriesPerClass[key];
        int fp = 0;

        System.out.println("Testing class: " + (key+1) + ", NumTests = " + numTests);
        int count = 0;
        for (int i = 0; i < CLASS_NUMBER; i++) {

            if (i == key)
                continue;

            for (int j = 0; j < samples[i].size(); j++) {
                String sample = samples[i].get(j);
                boolean isFalsePositive = true;

                for (int k = 0; k < testedFilter.getK().get(); k++) {
                    MurmurHash mh = new MurmurHash();
                    int hash = Math.abs(mh.hash(sample.getBytes(StandardCharsets.UTF_8), sample.length(), k)) % m;
                    isFalsePositive = isFalsePositive && testedFilter.getBitSet().get(hash);
                }

                if (isFalsePositive)
                    fp++;

                if (numTests == count) {
                    double ratio = (double)fp / (double) count;
                    System.out.println("Tested class " + (key+1) + " FP = " + fp + " RATIO = " + ratio);
                    return;
                }

                count++;
            }
        }
    }

    private static BloomFilter[] retrieveBloomFilters(String path, FileSystem fs, Configuration conf) throws IOException {
        List<String> files = getAllFilesInPath(new Path(path), fs);
        BloomFilter[] filtersArray = new BloomFilter[CLASS_NUMBER];
        for (int i = 0; i < CLASS_NUMBER; i++)
            filtersArray[i] = new BloomFilter();

        for(String f: files){
            if(f.contains("_SUCCESS") || f.contains("part"))
                continue;

            SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(new Path(f)));

            VIntWritable key = new VIntWritable();
            BloomFilter bf = new BloomFilter();
            reader.next(key, bf);
            System.out.println("Loaded filter " + key + " cardinality " + bf.getBitSet().cardinality() + " " + bf.getM().get());

            filtersArray[key.get()-1].setM(bf.getM());
            filtersArray[key.get()-1].setK(bf.getK());
            filtersArray[key.get()-1].setBitSet(bf.getBitSet());
        }

        return filtersArray;
    }

    private static ArrayList<String>[] fillSamples(String dataset, FileSystem fs) throws IOException {
        ArrayList<String>[] samples = new ArrayList[CLASS_NUMBER];
        for (int i = 0; i < CLASS_NUMBER; i++)
            samples[i] = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path("hdfs://hadoop-namenode:9820/user/hadoop/" + dataset))))) {
            String line;
            line = br.readLine();
            while (line != null) {
                StringTokenizer tok = new StringTokenizer(line);
                String title = tok.nextToken();

                int rating = (int) Math.round(Double.parseDouble(tok.nextToken()));
                samples[rating - 1].add(title);

                line = br.readLine();
            }
        }

        return samples;
    }

    private static int[] getEntriesPerClass(String dataset, FileSystem fs) throws IOException {
        int[] entriesPerClass = new int[CLASS_NUMBER];
        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path("hdfs://hadoop-namenode:9820/user/hadoop/" + dataset))))) {
            String line;
            line = br.readLine();
            while (line != null) {
                StringTokenizer tok = new StringTokenizer(line);
                tok.nextToken(); //skip title

                int rating = (int) Math.round(Double.parseDouble(tok.nextToken()));
                entriesPerClass[rating - 1]++;

                line = br.readLine();
            }
        }

        return entriesPerClass;
    }

    private static List<String> getAllFilesInPath(Path filePath, FileSystem fs) throws IOException {
        List<String> fileList = new ArrayList<>();
        FileStatus[] fileStatus = fs.listStatus(filePath);
        for (FileStatus fileStat : fileStatus) {
            if (fileStat.isDirectory()) {
                fileList.addAll(getAllFilesInPath(fileStat.getPath(), fs));
            } else {
                fileList.add(fileStat.getPath().toString());
            }
        }
        return fileList;
    }

}
