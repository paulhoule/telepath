package com.ontology2.telepath.mergeAgainstDBpedia;

import com.ontology2.bakemono.mapreduce.InputPath;
import com.ontology2.centipede.parser.ContextualConverter;
import com.ontology2.centipede.parser.HasOptions;
import com.ontology2.centipede.parser.Option;
import org.apache.hadoop.fs.Path;

import java.util.List;

public class MergeToolOptions implements HasOptions {
    @Option(name="R",description="number of reducers")
    public int reducerCount;

    @Option(description="input and output file default directory")
    public String dir;

    @InputPath(1)
    @Option(description="page ID file",contextualConverter=PathConverter.class)
    public String pageIds;

    @InputPath(2)
    @Option(description="transitive redirects file",contextualConverter=PathConverter.class)
    public String transitiveRedirects;

    @InputPath(3)
    @Option(description="input files",contextualConverter=PathConverter.class)
    public List<String> input;

    @Option(description="output files",contextualConverter=PathConverter.class)
    public String output;

    public static class PathConverter implements ContextualConverter<String> {
        public String convert(String value, HasOptions that) {
            String defaultDir=getDefaultDir((MergeToolOptions) that);

            if(defaultDir.isEmpty())
                return value;

            Path there=new Path(defaultDir,value);
            return there.toString();
        }

        public String getDefaultDir(MergeToolOptions that) {
            return that.dir;
        }
    }
}
