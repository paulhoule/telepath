package com.ontology2.topNForLang;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LangCount implements Writable {
    final Text lang;
    final VIntWritable count;

    public LangCount() {
        lang=new Text();
        count=new VIntWritable();
    };

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        lang.write(dataOutput);
        count.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        lang.readFields(dataInput);
        count.readFields(dataInput);
    }
}
