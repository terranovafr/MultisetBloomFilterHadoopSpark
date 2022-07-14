package it.unipi.dii.inginf.cc.mapReduceJ2;

import it.unipi.dii.inginf.cc.model.BloomFilter;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.*;
import java.util.BitSet;

public class ReducerBF2 extends Reducer<VIntWritable, BloomFilter, VIntWritable, BloomFilter> {
    private static MultipleOutputs mos;
    private static BitSet toWrite;
    private static BitSet tmpBitSet;
    private static BloomFilter bloomFilter;

    @Override
    public void setup(Context context) {
        mos = new MultipleOutputs(context);
    }

    @Override
    public void reduce(VIntWritable key, Iterable<BloomFilter> values, Context context) throws IOException, InterruptedException {
        toWrite = new BitSet();
        int k = 0, m = 0;
        while (values.iterator().hasNext()) {
            bloomFilter = values.iterator().next();
            tmpBitSet = bloomFilter.getBitSet();
            k = bloomFilter.getK().get();
            m = bloomFilter.getM().get();
            for (int i = tmpBitSet.nextSetBit(0); i >= 0 && i < m; i = tmpBitSet.nextSetBit(i+1)) {
                toWrite.set(i);
            }
        }
        bloomFilter = new BloomFilter();
        bloomFilter.setBitSet(toWrite);
        bloomFilter.setK(new VIntWritable(k));
        bloomFilter.setM(new VIntWritable(m));
        mos.write(key, bloomFilter, "rate" + key);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}
