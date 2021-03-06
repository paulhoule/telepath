package com.ontology2.telepath.mergeAgainstDBpedia;
import com.google.common.collect.Sets;
import com.ontology2.bakemono.joins.TaggedKeyPartitioner;
import com.ontology2.bakemono.joins.TaggedTextItem;
import com.ontology2.bakemono.joins.TaggedTextKeyGroupComparator;
import com.ontology2.bakemono.joins.TaggedTextKeySortComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.theories.suppliers.TestedOn;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;

import static junit.framework.TestCase.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("../applicationContext.xml")

public class TestTool {
    @Autowired
    MergeTool countDBpediaMerge;

    @Before
    public void setup() {
        countDBpediaMerge.setConf(new Configuration());
    }

    @Test
    public void configuresItself() throws IOException, IllegalAccessException, ClassNotFoundException {
        String args[] = {
            "-input",
            "/aligator",
            "-pageIds",
            "/bear",
            "-transitiveRedirects",
            "/civet",
            "-output",
            "/dingo",
            "-R",
            "800"
        };


        assertEquals(TaggedTextItem.class,countDBpediaMerge.getMapOutputKeyClass());
        assertEquals(TaggedTextItem.class,countDBpediaMerge.getMapOutputValueClass());
        Job j=countDBpediaMerge.createJob(args);
        Set<Path> inputPaths= Sets.newHashSet(countDBpediaMerge.getInputPaths());
        assertEquals(3,inputPaths.size());
        assertTrue(inputPaths.contains(new Path("/aligator")));
        assertTrue(inputPaths.contains(new Path("/bear")));
        assertTrue(inputPaths.contains(new Path("/civet")));
        assertFalse(inputPaths.contains(new Path("/dingo")));

        assertEquals(TaggedTextKeyGroupComparator.class, countDBpediaMerge.getGroupingComparatorClass());
        assertEquals(MergeMapper.class,j.getMapperClass());
        assertEquals(MergeReducer.class,j.getReducerClass());
        assertEquals(TaggedTextKeyGroupComparator.class, j.getGroupingComparator().getClass());
        assertEquals(TaggedKeyPartitioner.class,j.getPartitionerClass());
        assertEquals(TaggedTextKeySortComparator.class, j.getSortComparator().getClass());
    }

    @Test
    public void findsInputPaths() throws NoSuchFieldException {
        Map<Field,Integer> map=MergeTool.searchForInputPaths(MergeToolOptions.class);
        assertEquals(3,map.size());
        Field pageIds=MergeToolOptions.class.getField("pageIds");
        Field transitiveRedirects=MergeToolOptions.class.getField("transitiveRedirects");
        Field input=MergeToolOptions.class.getField("input");
        assertEquals((int) 1,(int) map.get(pageIds));
        assertEquals((int) 2,(int) map.get(transitiveRedirects));
        assertEquals((int) 3,(int) map.get(input));
    }
}
