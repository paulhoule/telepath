package com.ontology2.telepath.bloom;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.ontology2.bakemono.bloom.BloomReducer;
import com.ontology2.bakemono.mapreduce.SingleJobTool;
import com.ontology2.centipede.shell.UsageException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.bloom.BloomFilter;

import javax.annotation.Nullable;

public class BloomMergeTool extends SingleJobTool<BloomMergeOptions> {
    @Override
    protected void validateOptions() {
        if(options.k==0)
            throw new UsageException("You must specify -k,  the number of hash functions");

        getConf().set(BloomReducer.NB_HASH,Integer.toString(options.k));
    }

    @Override
    protected String getName() {
        return "bloomMerge";
    }

    @Override
    public Class getOptionsClass() {
        return BloomMergeOptions.class;
    }

    @Override
    protected Class<? extends InputFormat> getInputFormatClass() {
        return SequenceFileInputFormat.class;
    }

    @Override
    protected Class<? extends OutputFormat> getOutputFormatClass() {
        return SequenceFileOutputFormat.class;
    }

    @Override
    protected Class<? extends Mapper> getMapperClass() {
        return BloomMergeMapper.class;      // can we just use the identity mapper?
    }

    @Override
    protected Class<? extends Reducer> getReducerClass() {
        return BloomMergeReducer.class;      // can we just use the identity mapper?
    }

    @Override
    protected Class<? extends Writable> getMapOutputKeyClass() {
        return NullWritable.class;
    }

    @Override
    protected Class<? extends Writable> getMapOutputValueClass() {
        return BloomFilter.class;
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
        return 1;
    }

    @Override
    protected Path getOutputPath() {
        return new Path(options.output);
    }
}
