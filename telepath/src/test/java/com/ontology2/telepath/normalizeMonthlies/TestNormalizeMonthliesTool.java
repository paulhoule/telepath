package com.ontology2.telepath.normalizeMonthlies;

import com.ontology2.telepath.bloom.InBloomFilterMapper;
import com.ontology2.telepath.bloom.InBloomFilterTool;
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
public class TestNormalizeMonthliesTool {
    @Autowired
    NormalizeMonthliesTool normalizeMonthliesTool;

    @Before
    public void setup() {
        normalizeMonthliesTool.setConf(new Configuration());
    }

    @Test
    public void createsCorrectJobConfiguration() throws IOException, IllegalAccessException, ClassNotFoundException {
        Job job=normalizeMonthliesTool.createJob(new String[] {
                "-input",
                "/basekb-sandbox/someLocation",
                "-yrmo",
                "1941-12",
                "-output",
                "/basekb-sandbox/anotherLocation",
                "-R",
                "9999"
        });

        assertEquals(job.getMapperClass(),NormalizeMonthliesMapper.class);
        assertEquals("1941-12",job.getConfiguration().get(NormalizeMonthliesReducer.YRMO));
        assertEquals(9999,job.getNumReduceTasks());
    }
}
