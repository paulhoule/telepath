package com.ontology2.telepath.bloom;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.ontology2.bakemono.bloom.BloomReducer;
import com.ontology2.bakemono.configuration.HadoopTool;
import com.ontology2.bakemono.mapreduce.SingleJobTool;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileAsBinaryOutputFormat;
import org.apache.hadoop.util.bloom.BloomFilter;
import com.ontology2.centipede.shell.UsageException;

import javax.annotation.Nullable;

@HadoopTool("createBloomFilter")
public class CreateBloomFilterTool extends SingleJobTool<CreateBloomOptions> {


    @Override
    protected void validateOptions() {
        if(options.k==0)
            throw new UsageException("You must specify -k,  the number of hash functions");

        if(options.m==0)
            throw new UsageException("You must specify -m,  the size of the bloom filter");

        getConf().set(BloomReducer.NB_HASH,Integer.toString(options.k));
        getConf().set(BloomReducer.VECTOR_SIZE,Integer.toString(options.m));
    }


    @Override
    protected String getName() {
        return "createBloomFilter";
    }

    @Override
    public Class getOptionsClass() {
        return CreateBloomOptions.class;
    }

    @Override
    protected Class<? extends InputFormat> getInputFormat() {
        return KeyValueTextInputFormat.class;
    }

    @Override
    protected Class<? extends Mapper> getMapperClass() {
        return BloomMapper.class;
    }

    @Override
    protected Class<? extends Writable> getMapOutputKeyClass() {
        return Text.class;
    }

    @Override
    protected Class<? extends Writable> getMapOutputValueClass() {
        return NullWritable.class;
    }

    @Override
    protected Class<? extends Reducer> getReducerClass() {
        return BloomReducer.class;
    }

    @Override
    public Class<? extends Writable> getOutputKeyClass() {
        return NullWritable.class;
    }

    @Override
    public Class<? extends Writable> getOutputValueClass() {
        return BloomFilter.class;
    }

    @Override
    public Iterable<Path> getInputPaths() {
        return Iterables.transform(options.input, new Function<String, Path>() {
            @Nullable
            @Override
            public Path apply(@Nullable String input) {
                return new Path(input);
            }
        });
    }

    @Override
    public int getNumReduceTasks() {
        return options.reducerCount>0 ? options.reducerCount : 1;
    }

    @Override
    protected Path getOutputPath() {
        return new Path(options.output);
    }

    @Override
    protected Class<? extends OutputFormat> getOutputFormatClass() {
        return SequenceFileAsBinaryOutputFormat.class;
    }

}
