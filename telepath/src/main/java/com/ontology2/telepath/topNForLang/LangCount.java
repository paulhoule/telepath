package com.ontology2.telepath.topNForLang;

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

    public LangCount(String lang,int count) {
        this.lang=new Text(lang);
        this.count=new VIntWritable(count);
    };

    public Text getLang() {
        return lang;
    }

    public VIntWritable getCount() {
        return count;
    }

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

    @Override
    public boolean equals(Object that) {
        if (that==null)
            return false;

        if (!(that instanceof LangCount))
            return false;

        LangCount other=(LangCount) that;
        return this.lang.equals(other.lang) && this.count.equals(other.count);
    }
}
