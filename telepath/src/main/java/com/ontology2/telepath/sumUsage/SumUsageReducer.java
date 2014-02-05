package com.ontology2.telepath.sumUsage;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SumUsageReducer extends Reducer<Text,FloatWritable,Text,Text> {
    private static final String THIS="com.ontology2.telepath.sumUsage";
    public static final String PREDICATE=THIS+".predicate";
    String predicate;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        predicate=context.getConfiguration().get(PREDICATE);
    }

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float sum=0.0F;
        for(FloatWritable value:values)
            sum += value.get();

        StringBuffer remainder=new StringBuffer(predicate);
        remainder.append("\t\"");
        remainder.append(sum);
        remainder.append("\"^^<http://www.w3.org/2001/XMLSchema#float>\t.");
        context.write(
                key,
                new Text(remainder.toString())
        );
    }
}
