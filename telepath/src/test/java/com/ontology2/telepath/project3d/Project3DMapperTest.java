package com.ontology2.telepath.project3d;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class Project3DMapperTest {

    Project3DMapper mapper;
    Mapper.Context context;

    @Before
    public void setup() {
        mapper=new Project3DMapper();
        context=mock(Mapper.Context.class);
    }

    @Test
    public void enPassesThrough() throws IOException, InterruptedException {
        mapper.map(new LongWritable(1L),new Text("en Slapstick 778 1551"),context);
        verify(context).write(new Text("Slapstick"),new LongWritable(778));
    }

    @Test
    public void frPassesThrough() throws IOException, InterruptedException {
        mapper.map(new LongWritable(1L),new Text("fr Slapstick 778 1551"),context);
        verifyNoMoreInteractions(context);
    }

}
