package com.talentana.bigdata.titanic.age;



import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;



import java.io.IOException;

import java.util.StringTokenizer;



public class TitanicAgeMapper extends Mapper<LongWritable,Text,Text,IntWritable> {



    private static final String DELIMITER = ",";

    private Text theGender = new Text();

    private IntWritable theAge = new IntWritable();

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



        String[] record = value.toString().split(DELIMITER);

        int survived = Integer.parseInt(record[1]);

        String gender = record[4];



        if(survived == 1){

            int age = 0;

            try{

                age = Integer.parseInt(record[5]);

            }

            catch(NumberFormatException nfe){

                

            }

            

            theGender.set(gender);

            theAge.set(age);

            context.write(theGender,theAge);

        }

    }



    @Override

    protected void cleanup(Context context) throws IOException, InterruptedException {

        super.cleanup(context);

    }

}

