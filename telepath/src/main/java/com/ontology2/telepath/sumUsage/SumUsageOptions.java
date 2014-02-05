package com.ontology2.telepath.sumUsage;

import com.ontology2.bakemono.mapreduce.StoreAs;
import com.ontology2.centipede.parser.ContextualConverter;
import com.ontology2.centipede.parser.HasOptions;
import com.ontology2.centipede.parser.Option;
import com.ontology2.centipede.parser.Required;
import com.ontology2.telepath.projectNormalized3D.ProjectNormalized3DTool;
import org.apache.hadoop.fs.Path;

import java.util.List;

public class SumUsageOptions implements HasOptions {
    @Option(name="R",description="number of reducers")
    public int reducerCount;

    @StoreAs(SumUsageReducer.PREDICATE)
    @Option(description="RDF predicate",defaultValue="<http://rdf.basekb.com/public/subjectiveEye3D>")
    public String predicate;

    @Option(description="input and output file default directory")
    public String dir;

    @Option(description="input files",contextualConverter=PathConverter.class)
    public List<String> input;

    @Option(description="output files",contextualConverter=PathConverter.class)
    public String output;

    public static class PathConverter implements ContextualConverter<String> {
        public String convert(String value, HasOptions that) {
            String defaultDir=getDefaultDir((SumUsageOptions) that);

            if(defaultDir.isEmpty())
                return value;

            Path there=new Path(defaultDir,value);
            return there.toString();
        }

        public String getDefaultDir(SumUsageOptions that) {
            return that.dir;
        }
    }
}
