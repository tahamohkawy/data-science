package com.talentana.bigdata.titanic.byClass;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SurvivedByClassMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    private static final String DELIMITER = ",";
    private static final IntWritable ONE = new IntWritable(1);
    private Text theClass = new Text();

    private enum counters{TOTAL_RECORDS,BAD_RECORDS}

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //So we will receive each record in this format 1,0,3,"Braund Mr. Owen Harris",male,22,1,0,A/5 21171,7.25,,S,
        /*
        DATA SET DESCRIPTION
        Column 1 : PassengerId
        Column 2 : Survived  (survived=0 & died=1)
        Column 3 : Pclass
        Column 4 : Name
        Column 5 : Sex
        Column 6 : Age
        Column 7 : SibSp
        Column 8 : Parch
        Column 9 : Ticket
        Column 10 : Fare
        Column 11 : Cabin
        Column 12 : Embarked
        */

        //In this problem statement, we will find the number of people died or survived in each class with their genders and ages.

        String[] record = value.toString().split(DELIMITER);
        int survived = Integer.parseInt(record[1]);

        if(survived == 0){
            String pclass = record[2];
            theClass.set(pclass);
            context.write(theClass,ONE);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        super.run(context);
    }
}
