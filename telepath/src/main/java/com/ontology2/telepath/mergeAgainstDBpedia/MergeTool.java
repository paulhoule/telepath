package com.ontology2.telepath.mergeAgainstDBpedia;

import com.ontology2.bakemono.configuration.HadoopTool;
import com.ontology2.bakemono.mapreduce.SelfAwareTool;
import com.ontology2.centipede.parser.ContextualConverter;
import com.ontology2.centipede.parser.HasOptions;
import com.ontology2.centipede.parser.Option;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.util.List;

@HadoopTool("countDBpediaMerge")
public class MergeTool extends SelfAwareTool<MergeToolOptions> {
    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return TextInputFormat.class;
    }

}
