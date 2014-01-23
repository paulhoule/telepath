package com.ontology2.telepath.normalizeMonthlies;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.ontology2.bakemono.configuration.HadoopTool;
import com.ontology2.bakemono.mapreduce.SingleJobTool;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import javax.annotation.Nullable;

@HadoopTool("normalizeMonthlies")
public class NormalizeMonthliesTool extends SingleJobTool<NormalizeMonthliesOptions> {
    @Override
    protected void validateOptions() {
        getConf().set(NormalizeMonthliesReducer.YRMO,options.yrmo);
    }

    @Override
    protected String getName() {
        return "normalizeMonthliesTool";
    }

    @Override
    protected Class<? extends Mapper> getMapperClass() {
        return NormalizeMonthliesMapper.class;
    }

    @Override
    protected Class<? extends Reducer> getReducerClass() {
        return NormalizeMonthliesReducer.class;
    }

    @Override
    protected Class<? extends Writable> getMapOutputKeyClass() {
        return Text.class;
    }

    @Override
    protected Class<? extends Writable> getMapOutputValueClass() {
        return VIntWritable.class;
    }

    @Override
    public Class<? extends Writable> getOutputKeyClass() {
        return Text.class;
    }

    @Override
    public Class<? extends Writable> getOutputValueClass() {
        return Text.class;
    }

    @Override
    public Class getOptionsClass() {
        return NormalizeMonthliesOptions.class;
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
        return options.reducerCount;
    }

    @Override
    protected Path getOutputPath() {
        return new Path(options.output);
    }

    protected Class<? extends InputFormat> getInputFormatClass() {
        return KeyValueTextInputFormat.class;
    }
}

