package com.ontology2.telepath.mergeAgainstDBpedia;

import com.ontology2.bakemono.RecyclingIterable;
import com.ontology2.bakemono.joins.TaggedTextItem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TestReducer {
    MergeReducer reducer;
    Reducer.Context context;

    @Before
    public void setup() {
        reducer =new MergeReducer();
        context=mock(Reducer.Context.class);
    }

    @Test
    public void ignoresNonTopics() throws IOException, InterruptedException {
        RecyclingIterable<TaggedTextItem> that=new RecyclingIterable(TaggedTextItem.class,
            new TaggedTextItem("0.009",3)
        );

        reducer.reduce(new TaggedTextItem("<http://dbpedia.org/resource/Justin_Bieber_Die_Die_Die>", 3), that, context);
        verifyNoMoreInteractions(context);
    }

    @Test
    public void acceptsKnownTopics() throws IOException, InterruptedException {
        RecyclingIterable<TaggedTextItem> that=new RecyclingIterable(TaggedTextItem.class,
                new TaggedTextItem("",1),
                new TaggedTextItem("0.5",3)
        );

        reducer.reduce(new TaggedTextItem("<http://dbpedia.org/resource/Justin_Bieber>", 3), that, context);
        verify(context).write(
                new Text("<http://dbpedia.org/resource/Justin_Bieber>"),
                new Text("0.5")
        );
    }

    @Test
    public void rewritesAlongRedirects() throws IOException, InterruptedException {
        RecyclingIterable<TaggedTextItem> that=new RecyclingIterable(TaggedTextItem.class,
                new TaggedTextItem("<http://dbpedia.org/resource/Justin_Bieber>",2),
                new TaggedTextItem("0.05",3)
        );

        reducer.reduce(new TaggedTextItem("<http://dbpedia.org/resource/Never_Say_Never>", 3), that, context);
        verify(context).write(
                new Text("<http://dbpedia.org/resource/Justin_Bieber>"),
                new Text("0.05")
        );
    }
}
