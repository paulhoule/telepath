package com.ontology2.telepath.bloom;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import java.io.IOException;
import java.util.Arrays;

public class InBloomFilterMapper extends Mapper<Text,Text,Text,Text> {

    private static final String THIS="com.ontology2.telepath.bloom.InBloomFilterMapper";
    public static final String FILTER_PATH=THIS+".filterPath";

    // the actual size of the bloom filter can be determined when we load it,  why not the hash number?
    static final public String NB_HASH=THIS+".nbHash";
    static final public String HASH_TYPE=THIS+".hashType";
    protected BloomFilter filter=null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        context.getConfiguration().writeXml(System.out);
        Path filterPath=new Path(context.getConfiguration().get(FILTER_PATH));
        int nbHash=context.getConfiguration().getInt(NB_HASH, 0);
        int hashType= Hash.parseHashType(context.getConfiguration().get(HASH_TYPE, "murmur"));
        FileSystem fs=FileSystem.get(filterPath.toUri(),context.getConfiguration());
        SequenceFile.Reader reader=new SequenceFile.Reader(fs,filterPath,context.getConfiguration());
        NullWritable emptyKey=NullWritable.get();
        BloomFilter partFilter=new BloomFilter();
        while(reader.next(emptyKey,partFilter)) {
            if(filter==null) {
                filter=new BloomFilter(partFilter.getVectorSize(),nbHash,hashType);
            }
            filter.or(partFilter);
        }
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        if(filter.membershipTest(toKey(key))) {
            context.write(key,value);
        }
    }

    public static Key toKey(Text t) {
        return new Key(Arrays.copyOfRange(t.getBytes(), 0, t.getLength()));
    }

    public static Key toKey(String s) {
        return toKey(new Text(s));
    }
}
