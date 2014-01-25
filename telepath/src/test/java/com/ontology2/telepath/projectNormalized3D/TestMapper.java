package com.ontology2.telepath.projectNormalized3D;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.*;

public class TestMapper {
    ProjectNormalized3DMapper mapper;
    Mapper<LongWritable,Text,Text,FloatWritable>.Context context;

    @Before
    public void setup() {
        mapper=new ProjectNormalized3DMapper();
        mapper.siteNormalizationFactor.put("xiv",32.33F);
        mapper.siteNormalizationFactor.put("emhd",18.10F);
        mapper.siteNormalizationFactor.put("nflx",386.08F);
        context=mock(Mapper.Context.class);
    }

    @Test
    public void unknownSiteIsIgnored() throws IOException, InterruptedException {
        Text in=new Text("aapl ZMQRZ 55 88491");
        mapper.map(new LongWritable(9),in,context);
        verifyNoMoreInteractions(context);
    }

    @Test
    public void normalizesXiv() throws IOException, InterruptedException {
        Text in=new Text("xiv ZMQRZ 7 88491");
        mapper.map(new LongWritable(10),in,context);
        verify(context).write(new Text("xiv ZMQRZ"),new FloatWritable(7/32.33F));
        verifyNoMoreInteractions(context);
    }
}
