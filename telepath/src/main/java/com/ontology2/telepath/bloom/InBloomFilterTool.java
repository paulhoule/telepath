package com.ontology2.telepath.bloom;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.ontology2.bakemono.configuration.HadoopTool;
import com.ontology2.bakemono.mapreduce.SingleJobTool;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import javax.annotation.Nullable;

@HadoopTool("inBloomFilter")
public class InBloomFilterTool extends SingleJobTool<InBloomFilterOptions> {
    @Override
    protected String getName() {
        return "inBloomFilter";
    }

    @Override
    protected Class<? extends Mapper> getMapperClass() {
        return InBloomFilterMapper.class;
    }

    @Override
    protected Class<? extends Writable> getMapOutputKeyClass() {
        return Text.class;
    }

    @Override
    protected Class<? extends Writable> getMapOutputValueClass() {
        return Text.class;
    }

    @Override
    protected Class<? extends Reducer> getReducerClass() {
        return null;
    }

    @Override
    public Class<? extends Writable> getOutputKeyClass() {
        return null;
    }

    @Override
    public Class<? extends Writable> getOutputValueClass() {
        return null;
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
        return 0;
    }

    @Override
    protected Path getOutputPath() {
        return new Path(options.output);
    }

    @Override
    protected Class<? extends OutputFormat> getOutputFormatClass() {
        return TextOutputFormat.class;
    }

    @Override
    public Class getOptionsClass() {
        return InBloomFilterOptions.class;
    }
}
