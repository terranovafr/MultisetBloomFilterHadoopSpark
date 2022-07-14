package it.unipi.dii.inginf.cc.mapReduceJ2;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.hash.MurmurHash;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.StringTokenizer;

public class MapperBFs extends Mapper<Object, Text, VIntWritable, VIntWritable> {
    private static final VIntWritable ratingWritable = new VIntWritable();
    private static final VIntWritable hashValues = new VIntWritable();
    private static final MurmurHash murmurHash = new MurmurHash();
    private static  int k;

    @Override
    protected void setup(Context context) {
        k = context.getConfiguration().getInt("bloom.filter.K", 3);
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer dataIterator = new StringTokenizer(value.toString());
        String titleId = dataIterator.nextToken();
        int rating = (int) Math.round(Double.parseDouble(dataIterator.nextToken()));
        //skip counter of ratings
        dataIterator.nextToken();
        ratingWritable.set(rating);
        for (int i = 0; i < k; i++) {
            hashValues.set(Math.abs(murmurHash.hash(titleId.getBytes(StandardCharsets.UTF_8), titleId.length(), i)));
            context.write(ratingWritable, hashValues);
        }
    }
}
