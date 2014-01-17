package com.ontology2.telepath.bloom;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.bloom.BloomFilter;

import java.io.IOException;

public class BloomMergeMapper extends Mapper<NullWritable,BloomFilter,NullWritable,BloomFilter> {

    @Override
    protected void map(NullWritable key, BloomFilter value, Context context) throws IOException, InterruptedException {
        context.write(key,value);
    }
}
