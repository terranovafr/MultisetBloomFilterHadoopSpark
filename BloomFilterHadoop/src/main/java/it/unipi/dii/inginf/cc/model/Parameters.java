package it.unipi.dii.inginf.cc.model;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.VIntWritable;

public class Parameters extends ArrayWritable {

     public Parameters() {
         super(VIntWritable.class);
     }
}
