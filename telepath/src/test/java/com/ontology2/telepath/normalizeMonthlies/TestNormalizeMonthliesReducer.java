package com.ontology2.telepath.normalizeMonthlies;

import com.ontology2.bakemono.RecyclingIterable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.*;

public class TestNormalizeMonthliesReducer {
    NormalizeMonthliesReducer reducer;
    Reducer<Text,VIntWritable,Text,Text>.Context context;

    @Before
    public void setup() throws IOException, InterruptedException {
        reducer=new NormalizeMonthliesReducer();
        context=mock(Reducer.Context.class);
        Configuration configuration=new Configuration();
        configuration.set(NormalizeMonthliesReducer.YRMO,"1951-06");
        stub(context.getConfiguration()).toReturn(configuration);
        reducer.setup(context);
    }

    @Test
    public void tryOne() throws IOException, InterruptedException {
        Text inKey=new Text("qr");
        Iterable<VIntWritable> inValues=new RecyclingIterable(
            VIntWritable.class,
            new VIntWritable(76412),
            new VIntWritable(2),
            new VIntWritable(7)
        );

        reducer.reduce(inKey,inValues,context);

        Text outKey=new Text("1951-06 qr");
        Text outValue=new Text("3 76421");
        verify(context).write(outKey,outValue);
    }

}
