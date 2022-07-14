package it.unipi.dii.inginf.cc.mapReduceJ1;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

public class MapperParams extends Mapper<Object, Text, VIntWritable, VIntWritable> {
    private VIntWritable[] numberWritable = null;
    private static final int CLASS_NUMBER = 10;

    @Override
    public void setup(Context context) {
        numberWritable = new VIntWritable[CLASS_NUMBER];
        for (int i = 0; i < CLASS_NUMBER; i++)
            numberWritable[i] = new VIntWritable(0);
    }

    @Override
    public void map(Object key, Text value, Context context) {
        final StringTokenizer tokens = new StringTokenizer(value.toString());
        tokens.nextToken(); //skip title
        int rating = (int) Math.round(Double.parseDouble(tokens.nextToken())); //get rating
        tokens.nextToken(); //skip number of votes
        numberWritable[rating-1].set(numberWritable[rating-1].get() + 1); //in-mapper combiner
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(int i = 0; i < CLASS_NUMBER; i++) {
            context.write(new VIntWritable(i+1), numberWritable[i]);
        }
    }
}
