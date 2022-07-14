package it.unipi.dii.inginf.cc.mapReduceJ2;
import it.unipi.dii.inginf.cc.model.BloomFilter;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.*;
import java.util.BitSet;


public class ReducerBFs extends Reducer<VIntWritable, VIntWritable, VIntWritable, BloomFilter> {
    private static int k;
    private static MultipleOutputs mos;
    private static BitSet filterBitSet;
    private static BloomFilter writableFilter;

    @Override
    public void setup(Context context) {
        k = context.getConfiguration().getInt("bloom.filter.K", 3);
        mos = new MultipleOutputs(context);
    }

    @Override
    public void reduce(VIntWritable key, Iterable<VIntWritable> values, Context context) throws IOException, InterruptedException {
        int m = context.getConfiguration().getInt("bloom.filter." + key + ".M", 500000);
        filterBitSet = new BitSet(m);
        for (VIntWritable v: values) {
            filterBitSet.set(v.get() % m);
        }
        writableFilter = new BloomFilter();
        writableFilter.setBitSet(filterBitSet);
        writableFilter.setK(new VIntWritable(k));
        writableFilter.setM(new VIntWritable(m));
        mos.write(key, writableFilter, "rate" + key);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}
