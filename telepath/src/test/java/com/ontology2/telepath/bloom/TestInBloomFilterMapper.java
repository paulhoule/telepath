package com.ontology2.telepath.bloom;

import org.junit.Test;
import static com.ontology2.telepath.bloom.InBloomFilterMapper.*;
import static junit.framework.Assert.assertEquals;

public class TestInBloomFilterMapper {
    @Test
    public void parsesFilenames() {
        assertEquals("20080101-010000",extractDateTime("/bigdata/2008/2008-01/pagecounts-20080101-010000.gz"));
    }
}
