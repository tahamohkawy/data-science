package com.talentana.bigdata.titanic.byComposite;

import com.talentana.bigdata.model.TitanicValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StatusByClassGenderMapper2 extends Mapper<LongWritable,Text,Text,TitanicValue> {

    private static final String DELIMITER = ",";

    private Text theKey = new Text();
    private IntWritable theAge = new IntWritable();
    private Text theClass = new Text();
    private Text theGender = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] record = value.toString().split(DELIMITER);
        String survived = record[1];
        theKey.set(survived);

        TitanicValue titanicValue = new TitanicValue();
        String pclass = record[2];
        theClass.set(pclass);
        titanicValue.setPclass(theClass);

        String pgender = record[4];
        theGender.set(pgender);
        titanicValue.setPgender(theGender);

        int age = 0;
        if(record[5] != null && record[1].trim().length() > 0){
            age = Integer.parseInt(record[5]);
        }

        theAge.set(age);
        titanicValue.setPage(theAge);

        context.write(theKey,titanicValue);
    }
}
