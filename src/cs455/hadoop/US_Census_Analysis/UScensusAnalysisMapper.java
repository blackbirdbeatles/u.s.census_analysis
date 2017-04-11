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

        String summaryLevel = segment.substring(10,10+3);
        String logicalRecordPartNumber = segment.substring(24,24+4);
        String state = segment.substring(8,8+2);

        if (summaryLevel.equals("100")){

            //get all the fields of a segment


            //Q1 Tenure
            Tenure tenure = new Tenure();

            if (logicalRecordPartNumber.equals("2")) {
                IntWritable ownerOccupied = new IntWritable(Integer.valueOf(segment.substring(1803, 1803+9)));
                IntWritable renterOccupied = new IntWritable(Integer.valueOf(segment.substring(1812,1812+9)));
                tenure = new Tenure(ownerOccupied,renterOccupied);
            }




            //Q2:
            // population by sex
            // Gender by Marital Status

            PopulationBySex populationBySex = new PopulationBySex();
            GenderByMaritalStatus genderByMaritalStatus = new GenderByMaritalStatus();

            if (logicalRecordPartNumber.equals("1")) {

                IntWritable totalMen  = new IntWritable(Integer.valueOf(segment.substring(363,363+9)));
                IntWritable totalWomen = new IntWritable(Integer.valueOf(segment.substring(372,372+9)));
                populationBySex = new PopulationBySex(totalMen,totalWomen);

                IntWritable neverMarriedMen   = new IntWritable(Integer.valueOf(segment.substring(4422,4422+9)));
                IntWritable neverMarriedWomen = new IntWritable(Integer.valueOf(segment.substring(4467,4467+9)));
                genderByMaritalStatus = new GenderByMaritalStatus(neverMarriedMen,neverMarriedWomen);
                
            }

            //Q3-Q8 default sub-class initialization
            AgeDistributionByGender_Hispanic ageDistributionByGender_Hispanic = new AgeDistributionByGender_Hispanic();
            UrbanAndRuralHouseholds urbanAndRuralHouseholds = new UrbanAndRuralHouseholds();
            ValueOwnerOccupied valueOwnerOccupied = new ValueOwnerOccupied();
            ValueOfRental valueOfRental = new ValueOfRental();
            RoomNumberPerHouse roomNumberPerHouse = new RoomNumberPerHouse();
            StatePopulation statePopulation = new StatePopulation();
            ElderlyPeople elderlyPeople = new ElderlyPeople();

            Segment segmentObject = new Segment(new Text(state), tenure, populationBySex,genderByMaritalStatus,ageDistributionByGender_Hispanic,urbanAndRuralHouseholds,valueOwnerOccupied,valueOfRental,roomNumberPerHouse,statePopulation, elderlyPeople);
            context.write(new Text("U.S."), segmentObject);
        }
        //emit <"US", Segment.class >


    }
}
