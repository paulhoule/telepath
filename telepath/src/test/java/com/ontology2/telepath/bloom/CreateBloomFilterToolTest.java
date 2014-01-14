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

public class CreateBloomFilterToolTest {
    @Autowired
    CreateBloomFilterTool createBloomFilterTool;

    @Before
    public void setup() {
        createBloomFilterTool.setConf(new Configuration());
    }

    @Test
    public void createsCorrectJobConfiguration() throws IOException, IllegalAccessException, ClassNotFoundException {
        Job job=createBloomFilterTool.createJob(new String[] {
            "-input",
            "/basekb-sandbox/someLocation",
            "-R",
            "9500",
            "-k",
             "7",
            "-m",
            "1000000",
            "-output",
            "/basekb-sandbox/anotherLocation"
        });

        assertEquals(job.getMapperClass(),BloomMapper.class);
    }
}
