package it.unipi.dii.inginf.cc.app;

import it.unipi.dii.inginf.cc.mapReduceJ1.MapperParams;
import it.unipi.dii.inginf.cc.mapReduceJ1.ReducerParams;
import it.unipi.dii.inginf.cc.mapReduceJ2.*;
import it.unipi.dii.inginf.cc.model.BloomFilter;
import it.unipi.dii.inginf.cc.model.Parameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Application {
    private static final int REDUCER_NUMBER = 2;
    private static final int CLASS_NUMBER = 10;
    private static int k = -1;
    private static final int[] m = new int[CLASS_NUMBER];

    /**
     * The main method executes first a job to compute the M,K parameters for the bloom filters.
     * The first job output is written in the output folder specified by the user. The files in the output folder
     * are read and their contents are passed to the second job as Configuration variables.
     * Then the second job is scheduled, and filters are actually created and saved in the folder specified by the user
     * in ten different files (one for each filter).
     * If the first job fails, the second one is not scheduled.
     * @param args console arguments: input file, job1 output folder, job2 output folder, false positivity rate, number
     *             of lines in each InputSplit
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 5) {
            System.err.println("Usage: BloomFilters <input> <outputParams> <outputBFs> <p> <split_size>");
            System.exit(1);
        }


        System.out.println("args[0]: <input>=" + args[0]);
        System.out.println("args[1]: <outputParams>=" + args[1]);
        System.out.println("args[2]: <outputBF>=" + args[2]);
        System.out.println("args[3]: <p>=" + args[3]);
        System.out.println("args[4]: <nLines>=" + args[4]);

        if (!computeParametersJob(conf, args[0], args[1], Double.parseDouble(args[3]), Integer.parseInt(args[4])))
            System.exit(-1);

        readFirstJobOutputs(conf, args[1]);

        if (!createBloomFilters(conf, args[0], args[2], Integer.parseInt(args[4])))
            System.exit(-1);
    }


    /**
     * Creates a job to compute K and M for the bloom filters.
     * After setting the number of lines per InputSplit, number of reducers, required false positivity rate,
     * it waits for completion and returns a boolean indicating success or failure of the job.
     */
    private static boolean computeParametersJob(Configuration conf, String inPath, String outPath, double p, int nLines) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(conf, "ComputeParams");
        job.setJarByClass(Application.class);

        job.setMapOutputKeyClass(VIntWritable.class);
        job.setMapOutputValueClass(VIntWritable.class);

        job.setOutputKeyClass(VIntWritable.class);
        job.setOutputValueClass(Parameters.class);

        job.setMapperClass(MapperParams.class);
        job.setReducerClass(ReducerParams.class);

        job.setNumReduceTasks(REDUCER_NUMBER);

        NLineInputFormat.addInputPath(job, new Path(inPath));
        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", nLines);

        job.getConfiguration().setDouble("bloom.filter.P", p);

        FileOutputFormat.setOutputPath(job, new Path(outPath));
        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        return job.waitForCompletion(true);
    }

    /**
     * Creates a job to compute and write the bloom filters in ten different files.
     * After setting the number of lines per InputSplit, it writes all K and M values into the configuration
     * file for further retrieval by the MapReduce tasks.
     * Through the MultipleOutputs class it also prepares the ten files that will be used to save the bloom filters.
     */
    private static boolean createBloomFilters(Configuration conf, String inPath, String outPath, int nLines) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(conf, "createBloomFilters");
        job.setJarByClass(Application.class);

        job.setMapOutputKeyClass(VIntWritable.class);

        job.setMapOutputValueClass(VIntWritable.class);

        job.setOutputKeyClass(VIntWritable.class);
        job.setOutputValueClass(BloomFilter.class);

        job.setMapperClass(MapperBFs.class);
        job.setReducerClass(ReducerBFs.class);

        job.setNumReduceTasks(REDUCER_NUMBER);

        NLineInputFormat.addInputPath(job, new Path(inPath));
        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", nLines);

        System.out.println("**************K: "+ k);
        job.getConfiguration().setInt("bloom.filter.K", k);

        for(int i = 0; i < CLASS_NUMBER; i++) {
            job.getConfiguration().setInt("bloom.filter." + (i+1) + ".M", m[i]);
            System.out.println("***********M: "+ i + " " + m[i]);
        }

        FileOutputFormat.setOutputPath(job, new Path(outPath));
        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        for(int i = 0; i<m.length; i++) {
            MultipleOutputs.addNamedOutput(job, "rate" + (i + 1), SequenceFileOutputFormat.class,
                    VIntWritable.class, BloomFilter.class);
        }

        return job.waitForCompletion(true);
    }



    private static void readFirstJobOutputs(Configuration conf, String outputFolder) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        List<String> files = getAllFilesInPath(new Path(outputFolder), fs);

        for(String f: files){
            if(f.contains("_SUCCESS"))
                continue;

            SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                    SequenceFile.Reader.file(new Path(f)));
            VIntWritable key = new VIntWritable();
            Parameters value = new Parameters();

            while(reader.next(key, value)) {
                VIntWritable mWritable = (VIntWritable) value.get()[0];
                if(k == -1) {
                    VIntWritable kWritable = (VIntWritable) value.get()[1];
                    k = kWritable.get();
                }
                m[key.get()-1] = mWritable.get();
            }
        }
    }

    public static List<String> getAllFilesInPath(Path filePath, FileSystem fs) throws IOException {
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
