package com.ontology2.bakemono.mapreduce;

import com.google.common.collect.Lists;
import com.ontology2.bakemono.mapred.ToolBase;
import com.ontology2.centipede.parser.OptionParser;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

public abstract class SingleJobTool<OptionsClass> extends ToolBase {
    protected OptionsClass options;
    protected void validateOptions() {} ;
    abstract protected String getName();   // can this be defaulted with Spring magic?
    protected Class<? extends InputFormat> getInputFormatClass() {
        return TextInputFormat.class;
    }
    protected abstract Class<? extends Mapper> getMapperClass();

    // can the two following classes be defaulted by something that introspects
    // the mapper?

    protected abstract Class<? extends Writable> getMapOutputKeyClass();
    protected abstract Class<? extends Writable> getMapOutputValueClass();

    protected Class<? extends Reducer> getReducerClass() {
        return Reducer.class;
    }

    abstract public Class<? extends Writable> getOutputKeyClass();
    abstract public Class<? extends Writable> getOutputValueClass();

    abstract public Iterable<Path> getInputPaths();
    abstract public int getNumReduceTasks();
    protected abstract Path getOutputPath();
    protected Class<? extends OutputFormat> getOutputFormatClass() {
        return TextOutputFormat.class;
    }

    //
    // "null" means don't compress
    //

    protected Class<? extends CompressionCodec> getOutputCompressorClass() {
        return GzipCodec.class;
    }

    //
    // the assumption here is that any real instance of this will be a non-generic
    // subclass of a class that has the parameter filled in,  thus we can figure
    // out the class from the parameter.  If this assumption isn't true you
    // can subclass this.
    //
    
    abstract public Class getOptionsClass();

    //{
    //    return ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0].getClass();
    //}

    public int run(String[] strings) throws Exception {
        Job job = createJob(strings);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    //
    // this is public so it is accessible for testing...  that is,  we can test a dry run that
    // creates the job but doesn't run it
    //

    public Job createJob(String[] strings) throws IllegalAccessException, IOException {
        options=extractOptions(strings);
        validateOptions();
        configureOutputCompression();
        Job job=new Job(getConf(),getName());
        job.setJarByClass(getClass());
        job.setMapperClass(getMapperClass());
        job.setMapOutputKeyClass(getMapOutputKeyClass());
        job.setMapOutputValueClass(getMapOutputValueClass());
        job.setReducerClass(getReducerClass());
        job.setOutputKeyClass(getOutputKeyClass());
        job.setOutputValueClass(getOutputValueClass());

        job.setInputFormatClass(getInputFormatClass());
        for(Path p:getInputPaths()) {
            FileInputFormat.addInputPath(job, p);
        }

        job.setNumReduceTasks(getNumReduceTasks());
        FileOutputFormat.setOutputPath(job, getOutputPath());
        job.setOutputFormatClass(getOutputFormatClass());

        // should we let output compression be configurable?  the bloom filters shouldn't be compressible
        // if they are optimally tuned,  but trying to compress a file that size won't hurt

        if(getOutputCompressorClass()!=null) {
            FileOutputFormat.setCompressOutput(job,true);
            FileOutputFormat.setOutputCompressorClass(job,getOutputCompressorClass());
        }

        return job;
    }

    private OptionsClass extractOptions(String[] strings) throws IllegalAccessException {
        return extractOptions(Lists.newArrayList(strings));
    }

    private OptionsClass extractOptions(ArrayList<String> strings) throws IllegalAccessException {
        OptionParser parser=new OptionParser(getOptionsClass());
        applicationContext.getAutowireCapableBeanFactory().autowireBean(parser);

        return (OptionsClass) parser.parse(strings);
    }



}
