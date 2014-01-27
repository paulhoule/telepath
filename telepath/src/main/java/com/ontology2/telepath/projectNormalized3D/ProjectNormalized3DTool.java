package com.ontology2.telepath.projectNormalized3D;

import com.ontology2.bakemono.configuration.HadoopTool;
import com.ontology2.bakemono.mapreduce.SelfAwareTool;
import com.ontology2.centipede.parser.ContextualConverter;
import com.ontology2.centipede.parser.HasOptions;
import com.ontology2.centipede.parser.Option;
import org.apache.hadoop.fs.Path;

import java.util.List;

@HadoopTool("projectNormalized3D")
public class ProjectNormalized3DTool extends SelfAwareTool<ProjectNormalized3DOptions> {
    private static final String THIS="com.ontology2.telepath.projectNormalized3D";
    public static final String FACTORS=THIS+".factors";

}
