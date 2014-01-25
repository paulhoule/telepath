package com.ontology2.telepath.projectNormalized3D;

import com.ontology2.bakemono.RecyclingIterable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.theories.suppliers.TestedOn;

import java.io.IOException;

import static org.mockito.Mockito.*;

public class TestReducer {
    ProjectNormalized3DReducer reducer;
    Reducer<Text,FloatWritable,Text,Text>.Context context;

    @Before
    public void setup() {
        reducer=new ProjectNormalized3DReducer();
        context=mock(Reducer.Context.class);
    }

    @Test
    public void addEmUp() throws IOException, InterruptedException {
        Iterable<FloatWritable> values=new RecyclingIterable(
                FloatWritable.class,
                new FloatWritable(4.0F),
                new FloatWritable(17.0F),
                new FloatWritable(3.0F)
        );

        reducer.reduce(new Text("fl Orlando"),values,context);
        verify(context).write(new Text("fl Orlando"),new Text("24.0"));
    }
}
