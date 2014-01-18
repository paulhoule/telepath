package com.ontology2.telepath.project3d;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Project3DReducer extends Reducer<Text,LongWritable,Text,LongWritable> {

    private final static String THIS="com.ontology2.telepath.project3d";
    public static String THRESHOLD=THIS+".threshold";
    private int threshold;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        threshold=Integer.parseInt(context.getConfiguration().get(THRESHOLD));
    }

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum=0;
        for(LongWritable v:values) {
            sum += v.get();
        }

        if(sum>threshold) {
            context.write(key,new LongWritable(sum));
        }
    }
}
