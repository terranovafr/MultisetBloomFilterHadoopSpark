package it.unipi.dii.inginf.cc.model;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.BitSet;


public class BloomFilter implements Writable {
    private BytesWritable filter;
    private VIntWritable k;
    private VIntWritable m;

    public BloomFilter(){
        filter = new BytesWritable();
        k = new VIntWritable();
        m = new VIntWritable();
    }

    public BytesWritable getFilter() {
        return filter;
    }

    public void setFilter(BytesWritable filter) {
        this.filter = filter;
    }

    public VIntWritable getK() {
        return k;
    }

    public void setK(VIntWritable k) {
        this.k = k;
    }

    public VIntWritable getM() {
        return m;
    }

    public void setM(VIntWritable m) {
        this.m = m;
    }

    public BitSet getBitSet(){
        return BitSet.valueOf(filter.getBytes());
    }

    public void setBitSet(BitSet b) {
        byte[] arr = b.toByteArray();
        filter = new BytesWritable();
        filter.setSize(arr.length);
        filter.set(arr, 0, arr.length);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        filter.write(dataOutput);
        k.write(dataOutput);
        m.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        filter.readFields(dataInput);
        k.readFields(dataInput);
        m.readFields(dataInput);
    }

}
