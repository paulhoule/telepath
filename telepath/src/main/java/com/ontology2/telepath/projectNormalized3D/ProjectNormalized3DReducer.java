package com.ontology2.telepath.projectNormalized3D;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ProjectNormalized3DReducer extends
        Reducer<Text,FloatWritable,Text,Text> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float sum=0;
        for(FloatWritable count:values) {
            sum += count.get();
        }

        context.write(new Text(key),new Text(Float.toString(sum)));
    }
}
