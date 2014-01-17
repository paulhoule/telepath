package com.ontology2.telepath.bloom;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.hash.Hash;

import java.io.IOException;

public class BloomMergeReducer extends Reducer<NullWritable,BloomFilter,NullWritable,BloomFilter> {

    protected BloomFilter filter=null;
    private static final String THIS="com.ontology2.telepath.bloom.BloomMergeReducer";
    static final public String NB_HASH=THIS+".nbHash";
    static final public String HASH_TYPE=THIS+".hashType";
    Log LOG= LogFactory.getLog(BloomMergeReducer.class);
    private int nbHash;
    private int hashType;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        nbHash=context.getConfiguration().getInt(NB_HASH, 0);
        hashType= Hash.parseHashType(context.getConfiguration().get(HASH_TYPE, "murmur"));

    }

    @Override
    protected void reduce(NullWritable key, Iterable<BloomFilter> values, Context context) throws IOException, InterruptedException {
        for(BloomFilter partFilter:values) {
            if(filter==null) {
                filter=new BloomFilter(partFilter.getVectorSize(),nbHash,hashType);
            }
            filter.or(partFilter);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if(filter!=null)
            context.write(NullWritable.get(),filter);
    }
}
