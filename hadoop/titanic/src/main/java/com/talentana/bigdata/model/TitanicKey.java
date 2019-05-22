package com.talentana.bigdata.model;


import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.WritableComparable;


import java.io.DataInput;

import java.io.DataOutput;

import java.io.IOException;


public class TitanicKey implements WritableComparable<TitanicKey> {


    private Text status;

    private Text pclass;

    private Text pgender;


    public TitanicKey() {

        status = new Text();

        pclass = new Text();

        pgender = new Text();

    }


    public TitanicKey(Text status, Text pclass, Text pgender) {

        this.status = status;

        this.pclass = pclass;

        this.pgender = pgender;

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


    public Text getStatus() {

        return status;

    }


    public void setStatus(Text status) {

        this.status = status;

    }


    public void set(Text status, Text pclass, Text pgender) {

        this.status = status;

        this.pclass = pclass;

        this.pgender = pgender;

    }


    @Override

    public int compareTo(TitanicKey o) {

        /*if(o.status.compareTo(status) == 0){

            if(o.pclass.compareTo(pclass) == 0){

                return o.pgender.compareTo(pgender);

            }

        }

        if(o.pclass.compareTo(pclass) == 0){

            return o.pgender.compareTo(pgender);

        }



        return o.pgender.compareTo(pgender);*/


        return o.status.compareTo(status);

    }


    @Override

    public boolean equals(Object obj) {

        if (obj == null || !(obj instanceof TitanicKey)) return false;


        TitanicKey tk = (TitanicKey) obj;


        return tk.pclass.equals(pclass) && tk.pgender.equals(pgender) && tk.status.equals(status);

    }


    @Override

    public int hashCode() {

        return status.hashCode();

    }


    @Override

    public String toString() {

        return "[ status = " + status + " , pclass= " + pclass + " , pgender= " + pgender + "]";

    }


    @Override

    public void write(DataOutput dataOutput) throws IOException {

        status.write(dataOutput);

        pclass.write(dataOutput);

        pgender.write(dataOutput);

    }


    @Override

    public void readFields(DataInput dataInput) throws IOException {

        status.readFields(dataInput);

        pclass.readFields(dataInput);

        pgender.readFields(dataInput);

    }

}

