package com.ontology2.telepath.normalizeMonthlies;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.ontology2.bakemono.mapreduce.SingleJobTool;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import javax.annotation.Nullable;

public class NormalizeMonthliesTool extends SingleJobTool<NormalizeMonthliesOptions> {
    @Override
    protected String getName() {
        return "normalizeMonthliesTool";
    }

    @Override
    protected Class<? extends Mapper> getMapperClass() {
        return NormalizeMonthliesMapper.class;
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
        return NormalizeMonthliesTool.class;
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
        return null;
    }
}

