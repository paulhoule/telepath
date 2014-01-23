package com.ontology2.telepath.normalizeMonthlies;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TestNormalizeMonthliesMapper {
    NormalizeMonthliesMapper mapper;
    Mapper<Text,Text,Text,VIntWritable>.Context context;

    @Before
    public void setup() {
        mapper=new NormalizeMonthliesMapper();
        context=mock(Mapper.Context.class);
    }

    @Test
    public void tryOne() throws IOException, InterruptedException {
        Text inKey=new Text("zr PallaPalla");
        Text inValue=new Text("75312");

        mapper.map(inKey,inValue,context);
        verify(context).write(new Text("zr"),new VIntWritable(75312));
    }


}
