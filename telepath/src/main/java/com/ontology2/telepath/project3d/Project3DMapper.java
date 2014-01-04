package com.ontology2.telepath.project3d;

import com.google.common.base.Splitter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;

public class Project3DMapper extends Mapper<LongWritable,Text,Text,LongWritable> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Iterator<String> parts= Splitter.on(' ').split(value.toString()).iterator();
        String lang=parts.next();
        String page=parts.next();
        Long count=Long.parseLong(parts.next());

        if (!lang.equals("en"))
            return;

        context.write(new Text(page),new LongWritable(count));
    }
}
