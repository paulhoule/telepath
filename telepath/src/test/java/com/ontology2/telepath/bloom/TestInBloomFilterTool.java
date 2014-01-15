package com.ontology2.telepath.bloom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;

import static junit.framework.Assert.assertEquals;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("../applicationContext.xml")
public class TestInBloomFilterTool {
    @Autowired
    InBloomFilterTool inBloomFilterTool;

    @Before
    public void setup() {
        inBloomFilterTool.setConf(new Configuration());
    }

    @Test
    public void createsCorrectJobConfiguration() throws IOException, IllegalAccessException, ClassNotFoundException {
        Job job=inBloomFilterTool.createJob(new String[] {
                "-input",
                "/basekb-sandbox/someLocation",
                "-k",
                "7",
                "-output",
                "/basekb-sandbox/anotherLocation",
                "-bloomFilter",
                "/camels/"
        });

        assertEquals(job.getMapperClass(),InBloomFilterMapper.class);
        assertEquals(job.getConfiguration().get(InBloomFilterMapper.FILTER_PATH),"/camels/");
        assertEquals(job.getConfiguration().get(InBloomFilterMapper.NB_HASH),"7");
    }
}
