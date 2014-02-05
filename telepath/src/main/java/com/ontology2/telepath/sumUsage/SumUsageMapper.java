package com.ontology2.telepath.sumUsage;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SumUsageMapper extends Mapper<Text,Text,Text,FloatWritable> {
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        float f=Float.parseFloat(value.toString());
        context.write(key,new FloatWritable(f));
    }
}
