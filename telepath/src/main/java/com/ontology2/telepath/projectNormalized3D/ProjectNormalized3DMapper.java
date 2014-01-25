package com.ontology2.telepath.projectNormalized3D;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import static com.google.common.collect.Maps.*;
import static com.ontology2.bakemono.util.Utilities.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public class ProjectNormalized3DMapper extends Mapper<LongWritable,Text,Text,FloatWritable> {
    Log LOG= LogFactory.getLog(ProjectNormalized3DMapper.class);
    Map<String,Float> siteNormalizationFactor=newHashMap();

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
            Float normFactor=siteNormalizationFactor.get(lang);
            if(normFactor!=null) {
                FloatWritable output=new FloatWritable(count/normFactor);
                context.write(new Text(lang+" "+page),output);
            }

        } catch(NoSuchElementException |NumberFormatException nsee) {
            LOG.warn("invalid input line ["+value.toString()+"] in file "+context.getInputSplit().getLocations());
        }
    }
}
