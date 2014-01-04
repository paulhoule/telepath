package com.ontology2.telepath.project3d;

import com.ontology2.bakemono.RecyclingIterable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class Project3DReducerTest {
    private Project3DReducer reducer;
    private Reducer.Context context;

    @Before
    public void setup() {
        reducer=new Project3DReducer();
        context=mock(Reducer.Context.class);
    }

    @Test
    public void addsEmUp() throws IOException, InterruptedException {
        Iterable<LongWritable> source=new RecyclingIterable<>(
                LongWritable.class
                ,new LongWritable(12)
                ,new LongWritable(4)
                ,new LongWritable(1000));

        reducer.reduce(new Text("fl00k"),source,context);
        verify(context).write(new Text("fl00k"),new LongWritable(1016));
    }
}
