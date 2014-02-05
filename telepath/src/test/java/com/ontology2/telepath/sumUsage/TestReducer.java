package com.ontology2.telepath.sumUsage;

import com.ontology2.bakemono.RecyclingIterable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.*;

public class TestReducer {
    private SumUsageReducer reducer;
    private Reducer<Text,FloatWritable,Text,Text>.Context context;

    @Before
    public void setup() throws IOException, InterruptedException {
        reducer=new SumUsageReducer();
        context=mock(Reducer.Context.class);
        Configuration c=new Configuration();
        c.set(SumUsageReducer.PREDICATE,"<http://www.example.com/funktion>");
        stub(context.getConfiguration()).toReturn(c);
        reducer.setup(context);
    }

    @Test
    public void addThemUp() throws IOException, InterruptedException {
        Iterable<FloatWritable> those=new RecyclingIterable(FloatWritable.class,
                new FloatWritable(0.2F),
                new FloatWritable(0.7F),
                new FloatWritable(0.1F)
        );

        reducer.reduce(new Text("<http://www.example.com/Headknocker>"),those,context);
        verify(context).write(
                new Text("<http://www.example.com/Headknocker>"),
                new Text("<http://www.example.com/funktion>\t\"1.0\"^^<http://www.w3.org/2001/XMLSchema#float>\t.")
        );
    }
}
