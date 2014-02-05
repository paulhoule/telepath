package com.ontology2.telepath.sumUsage;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.theories.suppliers.TestedOn;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TestMapper {
    private SumUsageMapper mapper;
    private Mapper<Text,Text,Text,FloatWritable>.Context context;

    @Before
    public void setup() {
        mapper=new SumUsageMapper();
        context=mock(Mapper.Context.class);

    }

    @Test
    public void aSimpleCase() throws IOException, InterruptedException {
        mapper.map(new Text("<http://example.com/yfi>"),new Text("0.7"),context);
        verify(context).write(new Text("<http://example.com/yfi>"),new FloatWritable(0.7F));
    }
}
