package cs455.hadoop.US_Census_Analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by MyGarden on 4/11/17.
 */
/**
 * Combiner: Input to the combiner is the output from the mapper. It receives ("U.S.", SegmentObject ) pairs.
 * Sums up individual fields according to name of states. Emits ("U.S.", SegmentObject) pairs.
 */

public class UScensusAnalysisCombiner extends Reducer<Text, Segment, Text, Segment> {

        TreeMap<String, HashMap<String, Integer>> ans;
        ArrayList<String> problems;


        private void initializationOfAnswer(String state){
            //initial the various answer for one state
            HashMap<String, Integer> ansPerState = new HashMap<>();
            ans.put(state, ansPerState);

            //sum for Q1
            ansPerState.put("ownerOccupied", 0);
            ansPerState.put("renterOccupied", 0);


            //sum for Q2
            ansPerState.put("totalMen", 0);
            ansPerState.put("totalWomen", 0);

            ansPerState.put("neverMarriedMen", 0);
            ansPerState.put("neverMarriedWomen", 0);

            //sum for Q3
            ansPerState.put("aged18andBelow18Men", 0);
            ansPerState.put("aged19to29Men", 0);
            ansPerState.put("aged30to39Men", 0);
            ansPerState.put("agedAbove40Men", 0);

            ansPerState.put("aged18andBelow18Women", 0);
            ansPerState.put("aged19to29Women", 0);
            ansPerState.put("aged30to39Women", 0);
            ansPerState.put("agedAbove40Women", 0);


            //Q4:
            ansPerState.put("urbanHouseholds",0);
            ansPerState.put("ruralHouseholds",0);
            ansPerState.put("notDefined"     ,0);



            //TODO:Q5


            //problems insertion:
            // problems.add("percentage of residnce were rented vs. owned");



        /*
        //sum for Q4
        ansPerState.put("", 0);
        ansPerState.put("", 0);
        ansPerState.put("", 0);
        ansPerState.put("", 0);
        ansPerState.put("", 0);
        ansPerState.put("", 0);
        ansPerState.put("", 0);
        ansPerState.put("", 0);
        ansPerState.put("", 0);
        ansPerState.put("", 0);
        ansPerState.put("", 0);
        ansPerState.put("", 0);
        ansPerState.put("", 0);
        ansPerState.put("", 0);
        ansPerState.put("", 0);
        ansPerState.put("", 0);
        */
        }

        protected void reduce(Text key, Iterable<Segment> values, Context context) throws IOException, InterruptedException {
            //define the Hashtable ans
            ans = new TreeMap<>();


            //Go through the all segment object in current machine. generate hashmap with key of all states in U.S. Make sure all fields of a certain state sum up data from current machine segment.
            for (Segment seg : values) {
                String state = seg.getState().toString();
                if (!ans.containsKey(state))
                    initializationOfAnswer(state);
                HashMap<String,Integer> ansPerState = ans.get(state);

                //sum up the data in this segment into hashmap
                //Q1
                Tenure tenure = seg.getTenure();
                ansPerState.put("ownerOccupied", ansPerState.get("ownerOccupied") + tenure.getOwnerOccupied().get());
                ansPerState.put("renterOccupied", ansPerState.get("renterOccupied") + tenure.getRenterOccupied().get());

                //Q2:
                PopulationBySex populationBySex = seg.getPopulationBySex();
                ansPerState.put("totalMen", ansPerState.get("totalMen") + populationBySex.getTotalMen().get());
                ansPerState.put("totalWomen", ansPerState.get("totalWomen") + populationBySex.getTotalWomen().get());

                GenderByMaritalStatus genderByMaritalStatus = seg.getGenderByMaritalStatus();
                ansPerState.put("neverMarriedMen" ,ansPerState.get("neverMarriedMen" ) + genderByMaritalStatus.getNeverMarriedMen().get());
                ansPerState.put("neverMarriedWomen",ansPerState.get("neverMarriedWomen") + genderByMaritalStatus.getNeverMarriedWomen().get());

                //Q3:
                AgeDistributionByGender_Hispanic ageDistributionByGender_hispanic = seg.getAgeDistributionByGender_hispanic();
                ansPerState.put("aged18andBelow18Men"   ,ansPerState.get("aged18andBelow18Men"   ) + ageDistributionByGender_hispanic.getAged18andBelow18Men().get());
                ansPerState.put("aged19to29Men"         ,ansPerState.get("aged19to29Men"         ) + ageDistributionByGender_hispanic.getAged19to29Men().get());
                ansPerState.put("aged30to39Men"         ,ansPerState.get("aged30to39Men"         ) + ageDistributionByGender_hispanic.getAged30to39Men().get());
                ansPerState.put("agedAbove40Men"        ,ansPerState.get("agedAbove40Men"        ) + ageDistributionByGender_hispanic.getAgedAbove40Men().get());
                ansPerState.put("aged18andBelow18Women" ,ansPerState.get("aged18andBelow18Women" ) + ageDistributionByGender_hispanic.getAged18andBelow18Women().get());
                ansPerState.put("aged19to29Women"       ,ansPerState.get("aged19to29Women"       ) + ageDistributionByGender_hispanic.getAged19to29Women().get());
                ansPerState.put("aged30to39Women"       ,ansPerState.get("aged30to39Women"       ) + ageDistributionByGender_hispanic.getAged30to39Women().get());
                ansPerState.put("agedAbove40Women"      ,ansPerState.get("agedAbove40Women"      ) + ageDistributionByGender_hispanic.getAgedAbove40Women().get());

                //Q4:
                UrbanAndRuralHouseholds urbanAndRuralHouseholds = seg.getUrbanAndRuralHouseholds();
                ansPerState.put("urbanHouseholds",ansPerState.get("urbanHouseholds") + urbanAndRuralHouseholds.getUrbanHouseholds().get());
                ansPerState.put("ruralHouseholds",ansPerState.get("ruralHouseholds") + urbanAndRuralHouseholds.getRuralHouseholds().get());
                ansPerState.put("notDefined"     ,ansPerState.get("notDefined"     ) + urbanAndRuralHouseholds.getNotDefined().get());



            }


            //go through all states in hashmap, generate a largeSegment with data about that state


            //Q1


            //creat non-argument subclass
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


            //fill value into the subclass
            for (Map.Entry<String, HashMap<String,Integer>> entry : ans.entrySet()){


                Text state = new Text(entry.getKey());
                HashMap<String ,Integer> tableForOneState = entry.getValue();

                //get variable from table

                //Q1
                IntWritable renterOccupied = new IntWritable(tableForOneState.get("renterOccupied"));
                IntWritable ownerOccupied  = new IntWritable(tableForOneState.get("ownerOccupied") );
                tenure                     = new Tenure(ownerOccupied, renterOccupied);

                //Q2

                IntWritable totalMen   = new IntWritable(tableForOneState.get(  "totalMen"));
                IntWritable totalWomen = new IntWritable(tableForOneState.get("totalWomen"));
                populationBySex        = new PopulationBySex(totalMen, totalWomen);


                IntWritable neverMarriedMen   = new IntWritable(tableForOneState.get("neverMarriedMen")  );
                IntWritable neverMarriedWomen = new IntWritable(tableForOneState.get("neverMarriedWomen"));
                genderByMaritalStatus         = new GenderByMaritalStatus(neverMarriedMen,neverMarriedWomen);

                //Q3
                IntWritable aged18andBelow18Men   = new IntWritable(tableForOneState.get("aged18andBelow18Men"  ));
                IntWritable aged19to29Men         = new IntWritable(tableForOneState.get("aged19to29Men"        ));
                IntWritable aged30to39Men         = new IntWritable(tableForOneState.get("aged30to39Men"        ));
                IntWritable agedAbove40Men        = new IntWritable(tableForOneState.get("agedAbove40Men"       ));
                IntWritable aged18andBelow18Women = new IntWritable(tableForOneState.get("aged18andBelow18Women"));
                IntWritable aged19to29Women       = new IntWritable(tableForOneState.get("aged19to29Women"      ));
                IntWritable aged30to39Women       = new IntWritable(tableForOneState.get("aged30to39Women"      ));
                IntWritable agedAbove40Women      = new IntWritable(tableForOneState.get("agedAbove40Women"     ));
                ageDistributionByGender_Hispanic  = new AgeDistributionByGender_Hispanic(aged18andBelow18Men,aged19to29Men ,aged30to39Men, agedAbove40Men, aged18andBelow18Women, aged19to29Women, aged30to39Women, agedAbove40Women);


                //Q4
                IntWritable urbanHouseholds = new IntWritable(tableForOneState.get("urbanHouseholds"));
                IntWritable ruralHouseholds = new IntWritable(tableForOneState.get("ruralHouseholds"));
                IntWritable notDefined      = new IntWritable(tableForOneState.get("notDefined"     ));
                urbanAndRuralHouseholds = new UrbanAndRuralHouseholds(urbanHouseholds, ruralHouseholds, notDefined);



                //TODO:Q5

                //create new (larger) segment Object, and emit the pair ("U.S.", newSegmentObject) out
                Segment segmentObject = new Segment(state, tenure, populationBySex,genderByMaritalStatus,ageDistributionByGender_Hispanic,urbanAndRuralHouseholds,valueOwnerOccupied,valueOfRental,roomNumberPerHouse,statePopulation, elderlyPeople);
                context.write(new Text("U.S."), segmentObject);
            }






        } //close bracket for reduce()
}
