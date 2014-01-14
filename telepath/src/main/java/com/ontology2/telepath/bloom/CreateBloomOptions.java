package com.ontology2.telepath.bloom;

import com.ontology2.centipede.parser.ContextualConverter;
import com.ontology2.centipede.parser.HasOptions;
import com.ontology2.centipede.parser.Option;
import org.apache.hadoop.fs.Path;

import java.util.List;


// Note that these options are EXACTLY the same as Project3D options...  almost everything is going to have
// these,  aren't they?

public class CreateBloomOptions implements HasOptions {
    @Option(name="R",description="number of reducers")
    public int reducerCount;

    @Option(description="input and output file default directory")
    public String dir;

    @Option(description="input files",contextualConverter=PathConverter.class)
    public List<String> input;

    @Option(description="output files",contextualConverter=PathConverter.class)
    public String output;

    public static class PathConverter implements ContextualConverter<String> {
        public String convert(String value, HasOptions that) {
            String defaultDir=getDefaultDir((CreateBloomOptions) that);

            if(defaultDir.isEmpty())
                return value;

            Path there=new Path(defaultDir,value);
            return there.toString();
        }

        public String getDefaultDir(CreateBloomOptions that) {
            return that.dir;
        }
    }

    @Option(description="number of hash functions")
    public int k;

    @Option(description="size of bloom filter")
    public int m;
}
