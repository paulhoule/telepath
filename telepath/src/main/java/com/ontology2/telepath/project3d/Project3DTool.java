package com.ontology2.telepath.project3d;

import com.google.common.collect.Lists;
import com.ontology2.bakemono.configuration.HadoopTool;
import com.ontology2.bakemono.entityCentric.EntityCentricMapper;
import com.ontology2.bakemono.entityCentric.EntityIsAReducer;
import com.ontology2.bakemono.entityCentric.ExtractIsAOptions;
import com.ontology2.bakemono.mapred.ToolBase;
import com.ontology2.centipede.parser.OptionParser;
import com.ontology2.centipede.shell.UsageException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.util.List;

@HadoopTool("project3D")
public class Project3DTool extends ToolBase {
    @Override
    public int run(String[] strings) throws Exception {
        Project3DOptions options = extractOptions(Lists.newArrayList(strings));
        configureOutputCompression();
        getConf().set(Project3DReducer.THRESHOLD,Integer.toString(options.threshold));
        Job job=new Job(getConf(),"project3D");
        job.setJarByClass(this.getClass());
        job.setMapperClass(Project3DMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setReducerClass(Project3DReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        for(String path: options.input) {
            FileInputFormat.addInputPath(job, new Path(path));
        }

        job.setNumReduceTasks(options.reducerCount);
        FileOutputFormat.setOutputPath(job, new Path(options.output));
        FileOutputFormat.setCompressOutput(job,true);
        FileOutputFormat.setOutputCompressorClass(job,GzipCodec.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    Project3DOptions extractOptions(List<String> strings) throws IllegalAccessException {
        OptionParser parser=new OptionParser(Project3DOptions.class);
        applicationContext.getAutowireCapableBeanFactory().autowireBean(parser);

        Project3DOptions options=(Project3DOptions) parser.parse(Lists.newArrayList(strings));
        if (options.input.isEmpty())
            throw new UsageException("You did not specify a value for -input");

        if (options.output==null || options.output.isEmpty())
            throw new UsageException("You did not specify a value for -output");

        if(options.reducerCount<1) {
            options.reducerCount=1;
        }
        return options;
    }

}
