package it.unipi.dii.inginf.cc.mapReduceJ2;

import it.unipi.dii.inginf.cc.model.BloomFilter;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.hash.MurmurHash;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.StringTokenizer;

public class MapperBF2 extends Mapper<Object, Text, VIntWritable, BloomFilter> {
    private static final int[] m = new int[10];
    private static final BitSet[] filters = new BitSet[10];
    private static final MurmurHash murmurHash = new MurmurHash();
    private static int k;


    @Override
    protected void setup(Context context) {
        k = context.getConfiguration().getInt("bloom.filter.K", 3);
        for (int i = 0; i < 10; i++) {
            m[i] = context.getConfiguration().getInt("bloom.filter." + (i+1) + ".M", 50000);
            filters[i] = new BitSet(m[i]);
        }
    }

    @Override
    public void map(Object key, Text value, Context context) {
        StringTokenizer dataIterator = new StringTokenizer(value.toString());
        String titleId = dataIterator.nextToken();
        int rating = (int) Math.round(Double.parseDouble(dataIterator.nextToken()));
        //skip counter of ratings
        dataIterator.nextToken();
        for (int i = 0; i < k; i++) {
            int hash = Math.abs(murmurHash.hash(titleId.getBytes(StandardCharsets.UTF_8), titleId.length(), i)) % m[rating-1];
            filters[rating-1].set(hash);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (int i = 0; i < 10; i++) {
            BloomFilter f = new BloomFilter();
            f.setK(new VIntWritable(k));
            f.setM(new VIntWritable(m[i]));
            f.setBitSet(filters[i]);
            context.write(new VIntWritable(i+1), f);
        }
    }
}