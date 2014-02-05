package com.ontology2.telepath.sumUsage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.theories.suppliers.TestedOn;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("../applicationContext.xml")
public class TestTool {
    @Autowired SumUsageTool tool;

    @Before
    public void setup() {
        tool.setConf(new Configuration());
    }

    @Test
    public void trySimpleCase() throws IOException, IllegalAccessException {
        Job j=tool.createJob(new String[] {
            "-input",
            "/pu238",
            "-output",
            "/u233",
            "-R",
            "12"
        });

        assertEquals("<http://rdf.basekb.com/public/subjectiveEye3D>", j.getConfiguration().get(SumUsageReducer.PREDICATE));
    }
}
