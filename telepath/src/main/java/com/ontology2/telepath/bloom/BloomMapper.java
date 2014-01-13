package com.ontology2.telepath.bloom;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BloomMapper extends Mapper<Text,Text,Text,NullWritable> {

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
