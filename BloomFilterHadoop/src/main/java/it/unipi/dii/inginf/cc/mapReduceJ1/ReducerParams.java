package it.unipi.dii.inginf.cc.mapReduceJ1;

import it.unipi.dii.inginf.cc.model.Parameters;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;


public class ReducerParams extends Reducer<VIntWritable, VIntWritable, VIntWritable, Parameters> {
    private static double p;

    @Override
    public void setup(Context context) {
        p = context.getConfiguration().getDouble("bloom.filter.P", 0.1);
    }

    @Override
    public void reduce(VIntWritable rating, Iterable<VIntWritable> values, Context context) throws IOException, InterruptedException {
        int n = 0;
        double k, m;

        for (VIntWritable v: values) {
            n += v.get();
        }

        m = - (n * Math.log(p)) / Math.pow(Math.log(2), 2);
        k = m/n * Math.log(2);

        VIntWritable[] K_M = new VIntWritable[2]; //create array to store K and M values for this rating
        K_M[0] = new VIntWritable((int) Math.ceil(m));
        K_M[1] = new VIntWritable((int) Math.ceil(k));

        Parameters parameters = new Parameters();
        parameters.set(K_M); //store the array into our custom writable type
        context.write(rating, parameters);
    }
}
