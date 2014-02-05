package com.ontology2.telepath.sumUsage;


import com.ontology2.bakemono.configuration.HadoopTool;
import com.ontology2.bakemono.mapreduce.SelfAwareTool;

@HadoopTool("sumUsageTool")
public class SumUsageTool extends SelfAwareTool<SumUsageOptions> {
}
