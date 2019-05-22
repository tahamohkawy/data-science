package com.talentana.bigdata.titanic.byComposite;

import com.talentana.bigdata.model.TitanicKey;
import com.talentana.bigdata.utils.Constants;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StatusByClassGenderMapper extends Mapper<LongWritable,Text,TitanicKey,IntWritable> {

    private static final String DELIMITER = ",";
    private static final IntWritable ONE = new IntWritable(1);

    private Text theStatus = new Text();
    private Text theClass = new Text();
    private Text theGender = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] record = value.toString().split(DELIMITER);
        int survived = Integer.parseInt(record[1]);

        TitanicKey titanicKey = new TitanicKey();

        if(survived == 0){
            theStatus.set(Constants.Status.LIVE.toString());
            titanicKey.setStatus(theStatus);

            String pclass = record[2];
            theClass.set(pclass);
            titanicKey.setPclass(theClass);

            String pgender = record[4];
            theGender.set(pgender);
            titanicKey.setPgender(theGender);

            context.write(titanicKey,ONE);
        }
    }
}
