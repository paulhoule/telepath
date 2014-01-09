package com.ontology2.telepath.project3d;

import com.google.common.base.Splitter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class Project3DMapper extends Mapper<LongWritable,Text,Text,LongWritable> {
    private Splitter SPACE_SPLITTER =Splitter.on(' ');
    Log LOG= LogFactory.getLog(Project3DMapper.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            Iterator<String> parts= SPACE_SPLITTER.split(value.toString()).iterator();
            String lang=parts.next();
            String page=parts.next();
            Long count=Long.parseLong(parts.next());

            if (!lang.equals("en"))
                return;

            context.write(new Text(page),new LongWritable(count));
        } catch(NoSuchElementException|NumberFormatException nsee) {
            LOG.warn("invalid input line ["+value.toString()+"] in file "+context.getInputSplit().getLocations());
        }
    }
}
