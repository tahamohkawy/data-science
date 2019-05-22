package com.talentana.bigdata.model;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TitanicValue implements Writable {

    private Text pclass;
    private Text pgender;
    private IntWritable page;

    public TitanicValue() {
        pclass = new Text();
        pgender = new Text();
        page = new IntWritable();
    }

    public TitanicValue(Text pclass, Text pgender, IntWritable page) {
        this.pclass = pclass;
        this.pgender = pgender;
        this.page = page;
    }

    public Text getPclass() {
        return pclass;
    }

    public void setPclass(Text pclass) {
        this.pclass = pclass;
    }

    public Text getPgender() {
        return pgender;
    }

    public void setPgender(Text pgender) {
        this.pgender = pgender;
    }

    public IntWritable getPage() {
        return page;
    }

    public void setPage(IntWritable page) {
        this.page = page;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        pclass.write(dataOutput);
        pgender.write(dataOutput);
        page.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        pclass.readFields(dataInput);
        pgender.readFields(dataInput);
        page.readFields(dataInput);
    }
}
