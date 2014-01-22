package com.ontology2.telepath.normalizeMonthlies;

import com.google.common.base.Splitter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;

public class NormalizeMonthliesMapper extends Mapper<Text,Text,Text,VIntWritable> {

    final static Splitter SPACE_SPLITTER=Splitter.on(",");

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String keyString=key.toString();
        Iterator<String> parts=SPACE_SPLITTER.split(keyString).iterator();
        String siteId=parts.next();
        String uri=parts.next();
        int count=Integer.parseInt(value.toString());
        context.write(new Text(siteId),new VIntWritable(count));
    }
}
