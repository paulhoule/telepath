package com.ontology2.telepath.projectNormalized3D;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import static com.google.common.collect.Maps.*;
import static com.ontology2.bakemono.util.Utilities.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class ProjectNormalized3DMapper extends Mapper<LongWritable,Text,Text,FloatWritable> {
    Log LOG= LogFactory.getLog(ProjectNormalized3DMapper.class);
    Map<String,Float> siteNormalizationFactor=newHashMap();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        loadNormalizationFactors(context);
    }

    void loadNormalizationFactors(Context context) throws IOException {
        String systemPath=context.getConfiguration().get(ProjectNormalized3DTool.FACTORS);
        if(systemPath==null)
            return;

        loadNormalizationFactors(context, new Path(systemPath));
    }

    void loadNormalizationFactors(Context context,Path path) throws IOException {
        FileSystem fs=FileSystem.get(path.toUri(), context.getConfiguration());
        InputStream in=fs.open(path);
        loadNormalizationFactors(in,getYrmo(context));
    }

    String getYrmo(Context context) {
        FileSplit split=(FileSplit) context.getInputSplit();
        String name=split.getPath().toString();
        List<String> parts=SLASH_SPLITTER.splitToList(name);
        return parts.get(parts.size()-2);
    }

    void loadNormalizationFactors(InputStream in, String thisYrmo) throws IOException {
        thisYrmo=thisYrmo.intern();
        BufferedReader r=new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
        while(true) {
            String line=r.readLine();
            if(line==null)
                break;

            if(line.startsWith("#"))
                continue;

            Iterator<String> parts=WHITESPACE_SPLITTER.split(line).iterator();
            String yrmo=parts.next().intern();
            if(yrmo==thisYrmo) {
                String site=parts.next();
                int cntURIs=Integer.parseInt(parts.next());
                float cntHits=(float) Long.parseLong(parts.next());
                siteNormalizationFactor.put(site,cntHits);
            }
        }
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
