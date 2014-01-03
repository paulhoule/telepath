package com.ontology2.com.ontology2.telepath;

import com.google.common.collect.Lists;
import com.ontology2.bakemono.MainBase;

import java.util.List;

public class Main extends MainBase {
    public Main(String[] arg0) {
        super(arg0);
    }

    public List<String> getApplicationContextPath() {
        return Lists.newArrayList(
                "com/ontology2/bakemono/applicationContext.xml",
                "com/ontology2/telepath/applicationContext.xml"
        );
    }

    public static void main(String[] arg0) throws Exception {
        new Main(arg0).run();
    }
}
