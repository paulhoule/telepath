package com.ontology2.telepath.projectNormalized3D;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.io.InputStream;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("../applicationContext.xml")
public class TestMapper {
    ProjectNormalized3DMapper mapper;
    Mapper<LongWritable,Text,Text,FloatWritable>.Context context;
    @Autowired ApplicationContext applicationContext;

    @Before
    public void setup() {
        mapper=new ProjectNormalized3DMapper();
        mapper.siteNormalizationFactor.put("xiv",32.33F);
        mapper.siteNormalizationFactor.put("emhd",18.10F);
        mapper.siteNormalizationFactor.put("nflx",386.08F);
        context=mock(Mapper.Context.class);
        FileSplit split=mock(FileSplit.class);
        stub(split.getPath()).toReturn(new Path("/funky/2008-01/pagecount-blah200901831234.gz"));
        stub(context.getInputSplit()).toReturn(split);
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

    @Test
    public void loadNormalizationFactorsFromResource() throws IOException {
        mapper.siteNormalizationFactor.clear();
        InputStream in=applicationContext.getResource("com/ontology2/telepath/projectNormalized3D/normalizationSample.txt").getInputStream();
        mapper.loadNormalizationFactors(in,"2008-01");
        assertEquals(2,mapper.siteNormalizationFactor.size());
        assertEquals(1024.0F,mapper.siteNormalizationFactor.get("ba"),0.001F);
        assertEquals(99.0F,mapper.siteNormalizationFactor.get("be"),0.001F);
    }

    @Test
    public void getsYrmo() {
        assertEquals("2008-01",mapper.getYrmo(context));
    }
}
