package cs455.hadoop.US_Census_Analysis;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

        //get  summaryLevel, part number, state
        String summaryLevel = segment.substring(10,10+3);
        String logicalRecordPartNumber = segment.substring(24,24+4);
        String state = segment.substring(8,8+2);

        //If record is on the level we want , get all the fields of a segment
        if (summaryLevel.equals("100")){

            //Q1-Q8 default sub-class initialization
            Tenure tenure = new Tenure();
            PopulationBySex populationBySex = new PopulationBySex();
            GenderByMaritalStatus genderByMaritalStatus = new GenderByMaritalStatus();
            AgeDistributionByGender_Hispanic ageDistributionByGender_Hispanic = new AgeDistributionByGender_Hispanic();
            UrbanAndRuralHouseholds urbanAndRuralHouseholds = new UrbanAndRuralHouseholds();
            ValueOwnerOccupied valueOwnerOccupied = new ValueOwnerOccupied();
            ValueOfRental valueOfRental = new ValueOfRental();
            RoomNumberPerHouse roomNumberPerHouse = new RoomNumberPerHouse();
            StatePopulation statePopulation = new StatePopulation();
            ElderlyPeople elderlyPeople = new ElderlyPeople();

            //Q1:Tenure
            if (logicalRecordPartNumber.equals("0002")) {

                IntWritable ownerOccupied = new IntWritable(Integer.valueOf(segment.substring(1803, 1803+9)));
                IntWritable renterOccupied = new IntWritable(Integer.valueOf(segment.substring(1812,1812+9)));
                tenure = new Tenure(ownerOccupied,renterOccupied);

            }

            //Q2:population by sex，Gender by Marital Status
            if (logicalRecordPartNumber.equals("0001")) {

                IntWritable totalMen  = new IntWritable(Integer.valueOf(segment.substring(363,363+9)));
                IntWritable totalWomen = new IntWritable(Integer.valueOf(segment.substring(372,372+9)));
                populationBySex = new PopulationBySex(totalMen,totalWomen);

                IntWritable neverMarriedMen   = new IntWritable(Integer.valueOf(segment.substring(4422,4422+9)));
                IntWritable neverMarriedWomen = new IntWritable(Integer.valueOf(segment.substring(4467,4467+9)));
                genderByMaritalStatus = new GenderByMaritalStatus(neverMarriedMen,neverMarriedWomen);
                
            }

            //Q3: Hispanic age distribution by gender
            if (logicalRecordPartNumber.equals("0001")) {

                //pre-process for aged18andBelow18Men, aged18andBelow18Women
                int a18andBelow18m = 0, a18andBelow18w = 0;
                for (int i = 3864; i <=3972; i+=9)   {
                    a18andBelow18m += Integer.valueOf(segment.substring(i, i+9));
                    a18andBelow18w+= Integer.valueOf(segment.substring(i+279, i+279+9));
                }

                //pre-process for aged19to29Men, aged19to29Women
                int a19to29m = 0, a19to29w = 0;
                for (int i = 3981; i <=4017; i+=9)   {
                    a19to29m += Integer.valueOf(segment.substring(i, i+9));
                    a19to29w += Integer.valueOf(segment.substring(i+279, i+279+9));
                }

                //pre-process for aged30to39Men, aged30to39Women
                int a30to39m = 0, a30to39w = 0;
                for (int i = 4026; i <=4035; i+=9)   {
                    a30to39m += Integer.valueOf(segment.substring(i, i+9));
                    a30to39w += Integer.valueOf(segment.substring(i+279, i+279+9));
                }

                IntWritable aged18andBelow18Men        = new IntWritable(a18andBelow18m);
                IntWritable aged19to29Men              = new IntWritable(a19to29m);
                IntWritable aged30to39Men              = new IntWritable(a30to39m);

                IntWritable aged18andBelow18Women      = new IntWritable(a18andBelow18w);
                IntWritable aged19to29Women            = new IntWritable(a19to29w);
                IntWritable aged30to39Women            = new IntWritable(a30to39w);

                ageDistributionByGender_Hispanic = new AgeDistributionByGender_Hispanic(aged18andBelow18Men,aged19to29Men ,aged30to39Men, aged18andBelow18Women, aged19to29Women, aged30to39Women);

            }

            //create the Segmen object, emit <"U.S.", segmentObject>
            Segment segmentObject = new Segment(new Text(state), tenure, populationBySex,genderByMaritalStatus,ageDistributionByGender_Hispanic,urbanAndRuralHouseholds,valueOwnerOccupied,valueOfRental,roomNumberPerHouse,statePopulation, elderlyPeople);
            context.write(new Text("U.S."), segmentObject);
        }

    }
}
