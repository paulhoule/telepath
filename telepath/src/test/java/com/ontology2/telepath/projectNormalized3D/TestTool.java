package com.ontology2.telepath.projectNormalized3D;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.experimental.theories.suppliers.TestedOn;
import org.junit.runner.RunWith;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static junit.framework.TestCase.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("../applicationContext.xml")

public class TestTool {
    @Autowired
    ProjectNormalized3DTool projectNormalized3D;

    @Test
    public void itGetsWired() {
        projectNormalized3D.setConf(new Configuration());
        assertNotNull(projectNormalized3D);
    }

    @Test
    public void createAJob() throws IOException, IllegalAccessException, ClassNotFoundException {
        Job job=projectNormalized3D.createJob(new String[] {
                "-input",
                "/basekb-sandbox/someLocation",
                "-R",
                "9",
                "-output",
                "/basekb-sandbox/anotherLocation",
                "-factors",
                "/whereDyouGetThatFunkFrom"
        });

        assertEquals(ProjectNormalized3DMapper.class,job.getMapperClass());
        assertEquals(ProjectNormalized3DReducer.class,job.getReducerClass());
        assertEquals(1, FileInputFormat.getInputPaths(job).length);
        assertEquals(new Path("file:/basekb-sandbox/someLocation"),FileInputFormat.getInputPaths(job)[0]);
        assertEquals(new Path("/basekb-sandbox/anotherLocation"), FileOutputFormat.getOutputPath(job));
        assertEquals(9,job.getNumReduceTasks());
        assertEquals("/whereDyouGetThatFunkFrom",job.getConfiguration().get(ProjectNormalized3DTool.FACTORS));
    }
}
