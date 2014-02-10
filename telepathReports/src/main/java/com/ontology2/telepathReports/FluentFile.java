package com.ontology2.telepathReports;

import com.google.common.base.Charsets;

import java.io.*;

public class FluentFile extends File {
    public FluentFile(String basePath) {
        super(basePath);
    }

    public FluentFile(File basePath,String theRest) {
        super(basePath,theRest);
    }

    public FluentFile slash(String s) {
        return new FluentFile(this,s);
    }

    public OutputStream outputStream() throws FileNotFoundException {
        return new FileOutputStream(this);
    }

    public Writer writer() throws FileNotFoundException {
        return new OutputStreamWriter(outputStream(), Charsets.UTF_8);
    }

    public void ensureDir() throws FileNotFoundException {
        File parentFile=getParentFile();
        parentFile.mkdirs();
    }
}
