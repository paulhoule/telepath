package com.ontology2.telepath.bloom;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class BloomMapperTest {
    Mapper<Text,Text,Text,NullWritable>.Context context;
    BloomMapper mapper;

    @Before
    public void setup() {
        context=mock(Mapper.Context.class);
        mapper=new BloomMapper();
    }

    @Test
    public void tryIt() throws IOException, InterruptedException {
        mapper.map(new Text("love is"),new Text("going to save us"),context);
        verify(context).write(new Text("love is"), NullWritable.get());
    }
}
