package com.ontology2;

import java.util.List;
import com.google.common.collect.Lists;
import com.ontology2.centipede.shell.CentipedeShell;

public class Main extends CentipedeShell {

    public List<String> getApplicationContextPath() {
        String resourceDir="com.ontology2";
        return Lists.newArrayList(resourceDir.replace('.','/')+ "/applicationContext.xml");
    }

    @Override
    public String getShellName() {
        return "telepathReports";
    }

    public static void main(String[] args) {
        new Main().run(args);
    }
}
