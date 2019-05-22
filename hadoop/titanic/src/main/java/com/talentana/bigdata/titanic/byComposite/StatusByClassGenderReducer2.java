package com.talentana.bigdata.titanic.byComposite;

import com.talentana.bigdata.model.TitanicKey;
import com.talentana.bigdata.model.TitanicValue;
import com.talentana.bigdata.utils.Constants;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class StatusByClassGenderReducer2 extends Reducer<Text,TitanicValue,Text,IntWritable> {

    private Text theLiving = new Text("Total Living");
    private Text theDead = new Text("Total Dead");
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

    private IntWritable theLivingCount = new IntWritable();
    private IntWritable theDeadCount = new IntWritable();
    private IntWritable theLivingMalesCount = new IntWritable();
    private IntWritable theLivingFemalesCount = new IntWritable();
    private IntWritable theDeadMalesCount = new IntWritable();
    private IntWritable theDeadFemalesCount = new IntWritable();
    private IntWritable theLivingClass1Count = new IntWritable();
    private IntWritable theLivingClass2Count = new IntWritable();
    private IntWritable theLivingClass3Count = new IntWritable();
    private IntWritable theDeadClass1Count = new IntWritable();
    private IntWritable theDeadClass2Count = new IntWritable();
    private IntWritable theDeadClass3Count = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<TitanicValue> values, Context context) throws IOException, InterruptedException {

        int living = 0;
        int dead = 0;
        int livingMales = 0;
        int livingFemales = 0;
        int deadMales = 0;
        int deadFemales = 0;
        int livingClass1 = 0;
        int livingClass2 = 0;
        int livingClass3 = 0;
        int deadClass1 = 0;
        int deadClass2 = 0;
        int deadClass3 = 0;

        Iterator<TitanicValue> iter = values.iterator();

        while (iter.hasNext()){

            TitanicValue titanicValue = iter.next();

            //survived
            if(key.equals("0")) {

                living += 1;

                if(titanicValue.getPgender().toString().equals(Constants.Gender.male.name())){
                    livingMales += 1;
                }
                else{
                    livingFemales += 1;
                }

                if(titanicValue.getPclass().toString().equals(Constants.PClass.ONE.getValue())){
                    livingClass1 += 1;
                }
                else if(titanicValue.getPclass().toString().equals(Constants.PClass.TWO.getValue())){
                    livingClass2 += 1;
                }
                else{
                    livingClass3 += 1;
                }
            }
            //Dead
            else  {
                dead += 1;

                if(titanicValue.getPgender().toString().equals(Constants.Gender.male.name())){
                    deadMales += 1;
                }
                else{
                    deadFemales += 1;
                }
                if(titanicValue.getPclass().toString().equals(Constants.PClass.ONE.getValue())){
                    deadClass1 += 1;
                }
                else if(titanicValue.getPclass().toString().equals(Constants.PClass.TWO.getValue())){
                    deadClass2 += 1;
                }
                else{
                    deadClass3  += 1;
                }
            }
        }

        theLivingCount.set(living);
        theDeadCount.set(dead);
        theLivingMalesCount.set(livingMales);
        theLivingFemalesCount.set(livingFemales);
        theDeadMalesCount.set(deadMales);
        theDeadFemalesCount.set(deadFemales);
        theLivingClass1Count.set(livingClass1);
        theLivingClass2Count.set(livingClass2);
        theLivingClass3Count.set(livingClass3);
        theDeadClass1Count.set(deadClass1);
        theDeadClass2Count.set(deadClass2);
        theDeadClass3Count.set(deadClass3);


        context.write(theLiving,theLivingCount);

        context.write(theDead,theDeadCount);
        context.write(theLivingMales,theLivingMalesCount);
        context.write(theLivingFemales,theLivingFemalesCount);
        context.write(theDeadMales,theDeadMalesCount);
        context.write(theDeadFemales,theDeadFemalesCount);
        context.write(theLivingClass1,theLivingClass1Count);
        context.write(theLivingClass2,theLivingClass2Count);
        context.write(theLivingClass3,theLivingClass3Count);
        context.write(theDeadClass1,theDeadClass1Count);
        context.write(theDeadClass2,theDeadClass2Count);
        context.write(theDeadClass3,theDeadClass3Count);


    }
}
