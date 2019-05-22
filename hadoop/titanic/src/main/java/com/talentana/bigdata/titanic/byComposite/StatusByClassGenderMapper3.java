package com.talentana.bigdata.titanic.byComposite;

import com.talentana.bigdata.utils.Constants;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StatusByClassGenderMapper3 extends Mapper<LongWritable,Text,Text,IntWritable> {

    private static final String DELIMITER = ",";
    private static final IntWritable ONE = new IntWritable(1);

    private Text theStatus = new Text();
    private Text theClass = new Text();
    private Text theGender = new Text();

    private Text theLivingMales = new Text("Living Males");
    private Text theLivingFemales = new Text("Living Females");
    private Text theDeadMales = new Text("Dead Males");
    private Text theDeadFemales = new Text("Dead Females");
    private Text theLivingClass1 = new Text("Living Class1");
    private Text theLivingClass2 = new Text("Living Class2");
    private Text theLivingClass3 = new Text("Living Class3");
    private Text theDeadClass1 = new Text("Dead Class1");
    private Text theDeadClass2 = new Text("Dead Class2");
    private Text theDeadClass3 = new Text("Dead Class3");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] record = value.toString().split(DELIMITER);
        int survived = Integer.parseInt(record[1]);

        String pclass = record[2];
        theClass.set(pclass);

        String pgender = record[4];
        theGender.set(pgender);

        if(survived == 0){

            theStatus.set(Constants.Status.LIVE.toString());

            if(pgender.equals(Constants.Gender.male.name())){
                context.write(theLivingMales,ONE);
            }
            else{
                context.write(theLivingFemales,ONE);
            }

            if(pclass.equals(Constants.PClass.ONE.getValue())){
                context.write(theLivingClass1,ONE);
            }
            else if (pclass.equals(Constants.PClass.TWO.getValue())){
                context.write(theLivingClass2,ONE);
            }
            else{
                context.write(theLivingClass3,ONE);
            }
        }
        else{
            theStatus.set(Constants.Status.DEAD.toString());

            if(pgender.equals(Constants.Gender.male.name())){
                context.write(theDeadMales,ONE);
            }
            else{
                context.write(theDeadFemales,ONE);
            }

            if(pclass.equals(Constants.PClass.ONE.getValue())){
                context.write(theDeadClass1,ONE);
            }
            else if (pclass.equals(Constants.PClass.TWO.getValue())){
                context.write(theDeadClass2,ONE);
            }
            else{
                context.write(theDeadClass3,ONE);
            }
        }

        context.write(theStatus,ONE);
        context.write(theGender,ONE);
        context.write(theClass,ONE);

    }
}
