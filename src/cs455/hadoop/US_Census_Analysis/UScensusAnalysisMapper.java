package cs455.hadoop.US_Census_Analysis;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

/**
 * Mapper: Reads line by line. Emit <"US", segment> pairs.
 */
public class UScensusAnalysisMapper extends Mapper<LongWritable, Text, Text, Segment> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //read one line of segment
        String segment = value.toString();

        String summaryLevel = segment.substring(10,3);
        String logicalRecordPartNumber = segment.substring(24,4);
        String state = segment.substring(8,8+2);

        if (summaryLevel.equals("100")){

            //get all the fields of a segment


            //Q1 Tenure
            Tenure tenure = null;

            if (logicalRecordPartNumber.equals("2")) {
                IntWritable ownerOccupied = new IntWritable(Integer.valueOf(segment.substring(1803, 1803+9)));
                IntWritable renterOccupied = new IntWritable(Integer.valueOf(segment.substring(1812,1812+9)));
                tenure = new Tenure(ownerOccupied,renterOccupied);
            }




            //Q2:
            // population by sex
            // Gender by Marital Status

            PopulationBySex populationBySex = null;
            GenderByMaritalStatus genderByMaritalStatus = null;

            if (logicalRecordPartNumber.equals("1")) {

                IntWritable totalMen  = new IntWritable(Integer.valueOf(segment.substring(363,363+9)));
                IntWritable totalWomen = new IntWritable(Integer.valueOf(segment.substring(372,372+9)));
                populationBySex = new PopulationBySex(totalMen,totalWomen);

                IntWritable neverMarriedMen   = new IntWritable(Integer.valueOf(segment.substring(4422,4422+9)));
                IntWritable neverMarriedWomen = new IntWritable(Integer.valueOf(segment.substring(4467,4467+9)));
                IntWritable marriedMen        = new IntWritable(Integer.valueOf(segment.substring(4431,4431+9))+Integer.valueOf(segment.substring(4440,4440+9))+Integer.valueOf(segment.substring(4449,4449+9)));
                IntWritable marriedWomen      = new IntWritable(Integer.valueOf(segment.substring(4476,4476+9))+Integer.valueOf(segment.substring(4485,4485+9))+Integer.valueOf(segment.substring(4494,4494+9)));
                genderByMaritalStatus = new GenderByMaritalStatus(neverMarriedMen,marriedMen,neverMarriedWomen,marriedWomen);
                
            }


            Segment segment1 = new Segment(new Text(state), tenure, null,null,null,null,null,null,null,null);
            context.write(new Text("U.S."), segment1);
        }
        //emit <"US", Segment.class >


    }
}
