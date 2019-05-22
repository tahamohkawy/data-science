package com.talentana.bigdata.titanic.byComposite;

import com.talentana.bigdata.model.TitanicKey;
import com.talentana.bigdata.utils.Constants;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class StatusByClassGenderReducer extends Reducer<TitanicKey,IntWritable,Text,IntWritable> {

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
    protected void reduce(TitanicKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

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

        Iterator<IntWritable> iter = values.iterator();
        while (iter.hasNext()){
            IntWritable n = iter.next();

            if(key.getStatus().toString().equals(Constants.Status.LIVE.name())){
                living += n.get();

                if(key.getPgender().toString().equals(Constants.Gender.male.name())){
                    livingMales += n.get();
                }
                else{
                    livingFemales += n.get();
                }

                if(key.getPclass().toString().equals("1")){
                    livingClass1 += n.get();
                }
                else if(key.getPclass().toString().equals("2")){
                    livingClass2 += n.get();
                }
                else{
                    livingClass3 += n.get();
                }
            }
            else{
                dead += n.get();
                if(key.getPgender().toString().equals(Constants.Gender.male.name())){
                    deadMales += n.get();
                }
                else{
                    deadFemales += n.get();
                }

                if(key.getPclass().toString().equals("1")){
                    deadClass1 += n.get();
                }
                else if(key.getPclass().toString().equals("2")){
                    deadClass2 += n.get();
                }
                else{
                    deadClass3 += n.get();
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
