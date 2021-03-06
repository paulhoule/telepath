package com.ontology2.telepath.normalizeMonthlies;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NormalizeMonthliesReducer extends Reducer<Text,VIntWritable,Text,Text> {
    final static private String THIS="com.ontology2.telepath.normalizeMonthliesReducer";
    final static public String YRMO=THIS+".yrmo";

    String yrmo=null;
    @Override

    protected void setup(Context context) throws IOException, InterruptedException {
        yrmo=context.getConfiguration().get(YRMO);
    }

    @Override
    protected void reduce(Text key, Iterable<VIntWritable> values, Context context) throws IOException, InterruptedException {
        int uriCount=0;
        long viewCount=0;

        for(VIntWritable views:values) {
            uriCount++;
            viewCount+=views.get();
        }

        Text outKey=new Text(yrmo+" "+key.toString());
        Text outValue=new Text(uriCount+" "+viewCount);
        context.write(outKey,outValue);
    }
}
