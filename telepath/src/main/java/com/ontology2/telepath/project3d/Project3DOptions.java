package com.ontology2.telepath.project3d;

import com.ontology2.centipede.parser.ContextualConverter;
import com.ontology2.centipede.parser.HasOptions;
import com.ontology2.centipede.parser.Option;
import org.apache.hadoop.fs.Path;

import java.util.List;

public class Project3DOptions implements HasOptions {
    @Option(name="R",description="number of reducers")
    public int reducerCount;

    @Option(name="threshold",description="minimum number of views to acknowledge existence")
    public int threshold;

    @Option(description="input and output file default directory")
    public String dir;

    @Option(description="input files",contextualConverter=PathConverter.class)
    public List<String> input;

    @Option(description="output files",contextualConverter=PathConverter.class)
    public String output;

    public static class PathConverter implements ContextualConverter<String> {
        public String convert(String value, HasOptions that) {
            String defaultDir=getDefaultDir((Project3DOptions) that);

            if(defaultDir.isEmpty())
                return value;

            Path there=new Path(defaultDir,value);
            return there.toString();
        }

        public String getDefaultDir(Project3DOptions that) {
            return that.dir;
        }
    }
}
