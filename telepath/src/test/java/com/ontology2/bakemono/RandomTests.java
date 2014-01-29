package com.ontology2.bakemono;

import com.google.common.base.Joiner;
import org.junit.experimental.theories.suppliers.TestedOn;

import java.util.List;
import org.junit.Test;

import static com.google.common.collect.Lists.*;
import static java.lang.String.format;

public class RandomTests {
    public List<String> generateAllYrmo() {
        List<String> out=newArrayList();
        for(int i=2008;i<=2013;i++)
            for(int j=1;j<=12;j++) {
                out.add(format("%4d-%02d",i,j));
            }

        return out;
    }

    @Test
    public void allYrMo() {
        List<String> allYrMo=generateAllYrmo();
        System.out.print(Joiner.on(",").join(allYrMo));
    }
}
