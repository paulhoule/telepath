package com.ontology2.telepath.bloom;

import com.ontology2.centipede.parser.ContextualConverter;
import com.ontology2.centipede.parser.HasOptions;
import com.ontology2.centipede.parser.Option;
import org.apache.hadoop.fs.Path;

import java.util.List;

public class InBloomFilterOptions implements HasOptions {
    @Option(description="input and output file default directory")
    public String dir;

    @Option(description="input files",contextualConverter=PathConverter.class)
    public List<String> input;

    @Option(description="output files",contextualConverter=PathConverter.class)
    public String output;

    @Option(description="bloom filter",contextualConverter=PathConverter.class)
    public String bloomFilter;

    public static class PathConverter implements ContextualConverter<String> {
        public String convert(String value, HasOptions that) {
            String defaultDir=getDefaultDir((InBloomFilterOptions) that);

            if(defaultDir.isEmpty())
                return value;

            Path there=new Path(defaultDir,value);
            return there.toString();
        }

        public String getDefaultDir(InBloomFilterOptions that) {
            return that.dir;
        }
    }

    @Option(description="number of hash functions")
    public int k;
}
