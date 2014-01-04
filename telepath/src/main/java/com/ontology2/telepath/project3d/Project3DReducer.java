package com.ontology2.telepath.project3d;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Project3DReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum=0;
        for(LongWritable v:values) {
            sum += v.get();
        }

        context.write(key,new LongWritable(sum));
    }
}
