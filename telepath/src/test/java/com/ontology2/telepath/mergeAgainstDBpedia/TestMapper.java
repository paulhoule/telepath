package com.ontology2.telepath.mergeAgainstDBpedia;

import com.ontology2.bakemono.joins.TaggedTextItem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.theories.suppliers.TestedOn;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TestMapper {
    MergeMapper mapper;
    Mapper.Context context;

    @Before
    public void setup() {
        mapper=new MergeMapper();
        context=mock(Mapper.Context.class);
    }

    @Test
    public void testPageId() throws IOException, InterruptedException {
        mapper.currentTag=new VIntWritable(1);
        Text line=new Text("<http://dbpedia.org/resource/AccessibleComputing> <http://dbpedia.org/ontology/wikiPageID> \"10\"^^<http://www.w3.org/2001/XMLSchema#integer>");
        mapper.map(new LongWritable(101),line,context);
        verify(context).write(
                new TaggedTextItem("<http://dbpedia.org/resource/AccessibleComputing>",1),
                new TaggedTextItem("",1)
        );
    }

    @Test
    public void testRedirect() throws IOException, InterruptedException {
        mapper.currentTag=new VIntWritable(2);
        Text line=new Text("<http://dbpedia.org/resource/AsWeMayThink> <http://dbpedia.org/ontology/wikiPageRedirects> <http://dbpedia.org/resource/As_We_May_Think> .\n");
        mapper.map(new LongWritable(101),line,context);
        verify(context).write(
                new TaggedTextItem("<http://dbpedia.org/resource/AsWeMayThink>",2),
                new TaggedTextItem("<http://dbpedia.org/resource/As_We_May_Think>",2)
        );
    }

    @Test
    public void rejectsForeignKeys() throws IOException, InterruptedException {
        mapper.currentTag=new VIntWritable(3);
        Text line=new Text("fr Paris 0.2");
        mapper.map(new LongWritable(101),line,context);
        verifyNoMoreInteractions(context);
    }

    @Test
    public void rewritesEnKeys() throws IOException, InterruptedException {
        mapper.currentTag=new VIntWritable(3);
        Text line=new Text("en Paris 0.2");
        mapper.map(new LongWritable(101),line,context);
        verify(context).write(
                new TaggedTextItem("<http://dbpedia.org/resource/Paris>",3),
                new TaggedTextItem("0.2",3)
        );
    }
}
