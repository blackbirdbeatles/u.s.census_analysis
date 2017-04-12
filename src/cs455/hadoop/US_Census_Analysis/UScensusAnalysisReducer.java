package cs455.hadoop.US_Census_Analysis;

import com.sun.xml.internal.txw2.TXW;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives ("U.S.", SegmentObject ) pairs.
 * Sums up individual fields according to name of states. Emits (Name of State, List<Answer>) pairs.
 */
public class UScensusAnalysisReducer extends Reducer<Text, Segment, Text, Text> {

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



        //Q5:
        for (int i = 0 ; i < 20; i++)
            ansPerState.put("valueOwnerOccupied" + String.valueOf(i), 0);

        //Q6
        for (int i = 0 ; i < 16; i++)
            ansPerState.put("valueRent" + String.valueOf(i), 0);
        //Q7
        for (int i = 0 ; i < 9; i++) {
            ansPerState.put("distribution" + String.valueOf(i), 0);
            ansPerState.put("weightedPart" + String.valueOf(i), 0);
        }
        //Q8
        ansPerState.put("statePopulation", 0);
        ansPerState.put("elderlyPeople", 0);

        //Q9: initialize fields in hash map for one state to 0
        for (int i = 0 ; i < 3; i++)
            ansPerState.put("duration" + String.valueOf(i), 0);

        /*

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

        for (Segment seg : values) {
            String state = seg.getState().toString();
            if (!ans.containsKey(state))
                initializationOfAnswer(state);
            HashMap<String,Integer> ansPerState = ans.get(state);
            //Q1:
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

            //Q5:
            ArrayList<IntWritable> value = seg.getValueOwnerOccupied().getValue();
            for (int i = 0; i < 20; i++)
                ansPerState.put("valueOwnerOccupied" + String.valueOf(i), ansPerState.get("valueOwnerOccupied" + String.valueOf(i)) + value.get(i).get());


            //Q6
            ArrayList<IntWritable> valueR = seg.getValueOfRental().getValue();
            for (int i = 0; i < 16; i++)
                ansPerState.put("valueRent" + String.valueOf(i), ansPerState.get("valueRent" + String.valueOf(i)) + valueR.get(i).get());
            //Q7
            ArrayList<IntWritable> distribution = seg.getRoomNumberPerHouse().getDistriburion();
            ArrayList<IntWritable> weightedPart = seg.getRoomNumberPerHouse().getWeightedPart();
            for (int i = 0; i < 9; i++) {
                ansPerState.put("distribution" + String.valueOf(i), ansPerState.get("distribution" + String.valueOf(i)) + distribution.get(i).get());
                ansPerState.put("weightedPart" + String.valueOf(i), ansPerState.get("weightedPart" + String.valueOf(i)) + weightedPart.get(i).get());
            }

            //Q8:
            Integer statePopulation = seg.getStatePopulation().getStatePopulation().get();
            Integer above85   = seg.getElderlyPeople().getAgedOver85population().get();
            ansPerState.put("statePopulation", ansPerState.get("statePopulation") + statePopulation);
            ansPerState.put("elderlyPeople", ansPerState.get("elderlyPeople") + above85 );

            //Q9: calculation: add number in object to hashmap
            ArrayList<IntWritable> duration = seg.getVacancyDuration().getDuration();
            for (int i = 0; i < 3; i++)
                ansPerState.put("duration" + String.valueOf(i), ansPerState.get("duration" + String.valueOf(i)) + duration.get(i).get());
        }



        //Finally, print out answers
        Text spaceValue = new Text("");

        //Q1

        //print problem
        context.write(new Text("Q1: Percentage of residences were rented vs. owned"), spaceValue);


        for (Map.Entry<String, HashMap<String,Integer>> entry : ans.entrySet()){
            Text state = new Text(entry.getKey());
            HashMap<String ,Integer> tableForOneState = entry.getValue();

            //get variable from table
            Integer renterOccupied = tableForOneState.get("renterOccupied");
            Integer ownerOccupied = tableForOneState.get("ownerOccupied");

            //statistical arithmetics
            Integer totalResidence = renterOccupied + ownerOccupied;
            Double rentedRate = (renterOccupied*1.0 / totalResidence )*100;
            Double ownedRate  = (ownerOccupied*1.0  / totalResidence) *100;

            //convert to String str
            String strRented = Double.valueOf(rentedRate)+"%";
            String strOwned  = Double.valueOf(ownedRate) + "%";
            Text resultRented = new Text(strRented);
            Text resultOwned = new Text(strOwned);

            //result print (with format)
            context.write(state, spaceValue);
            context.write(new Text("    Rented"), resultRented);
            context.write(new Text("    Owned"), resultOwned);
        }

        //Q2

        //print problem
        context.write(new Text("Q2: Percentage of people never married"), spaceValue);

        for (Map.Entry<String, HashMap<String,Integer>> entry : ans.entrySet()){
            Text state = new Text(entry.getKey());
            HashMap<String ,Integer> tableForOneState = entry.getValue();

            //get variable from table
            Integer totalMen = tableForOneState.get(  "totalMen");
            Integer totalWomen = tableForOneState.get("totalWomen");
            Integer   neverMarriedMen = tableForOneState.get("neverMarriedMen");
            Integer neverMarriedWomen = tableForOneState.get("neverMarriedWomen");

            //statistical arithmetics
            Double singleMenRate   = (neverMarriedMen *1.0 / totalMen) * 100;
            Double singleWomenRate = (neverMarriedWomen *1.0 / totalWomen) * 100;


            //convert to String str
            String strMen = Double.valueOf(singleMenRate)+"%";
            String strWomen = Double.valueOf(singleWomenRate)+"%";
            Text resultMen = new Text(strMen);
            Text resultWomen = new Text(strWomen);


            //result print (with format)
            context.write(state, spaceValue);
            context.write(new Text("    Men"), resultMen);
            context.write(new Text("    Women"), resultWomen);
        }


        //Q3

        //print problem
        context.write(new Text("Q3: Hispanic age distribution by gender"), spaceValue);

        for (Map.Entry<String, HashMap<String,Integer>> entry : ans.entrySet()){
            Text state = new Text(entry.getKey());
            HashMap<String ,Integer> tableForOneState = entry.getValue();

            //get variable from table
            Integer aged18andBelow18Men   = tableForOneState.get("aged18andBelow18Men"  );
            Integer aged19to29Men         = tableForOneState.get("aged19to29Men"        );
            Integer aged30to39Men         = tableForOneState.get("aged30to39Men"        );
            Integer agedAbove40Men        = tableForOneState.get("agedAbove40Men");
            Integer aged18andBelow18Women = tableForOneState.get("aged18andBelow18Women");
            Integer aged19to29Women       = tableForOneState.get("aged19to29Women"      );
            Integer aged30to39Women       = tableForOneState.get("aged30to39Women"      );
            Integer agedAbove40Women      = tableForOneState.get("agedAbove40Women");
           


            //statistical arithmetics
            int totalMen = aged18andBelow18Men + aged19to29Men + aged30to39Men +agedAbove40Men;
            int totalWomen = aged18andBelow18Women + aged19to29Women + aged30to39Women +agedAbove40Women;
            Double a18MenRate   = (aged18andBelow18Men *1.0 / totalMen) * 100;
            Double a19MenRate   = (aged19to29Men *1.0 / totalMen) * 100;
            Double a30MenRate   = (aged30to39Men *1.0 / totalMen) * 100;

            Double a18WomenRate   = (aged18andBelow18Women *1.0 / totalWomen) * 100;
            Double a19WomenRate   = (aged19to29Women *1.0 / totalWomen) * 100;
            Double a30WomenRate   = (aged30to39Women *1.0 / totalWomen) * 100;


            //convert to String str
            String strA18Men = Double.valueOf(a18MenRate)+"%";
            String strA19Men = Double.valueOf(a19MenRate)+"%";
            String strA30Men = Double.valueOf(a30MenRate)+"%";

            String strA18Women = Double.valueOf(a18WomenRate)+"%";
            String strA19Women = Double.valueOf(a19WomenRate)+"%";
            String strA30Women = Double.valueOf(a30WomenRate)+"%";

            Text resultA18Men = new Text(strA18Men);
            Text resultA19Men = new Text(strA19Men);
            Text resultA30Men = new Text(strA30Men);

            Text resultA18Women = new Text(strA18Women);
            Text resultA19Women = new Text(strA19Women);
            Text resultA30Women = new Text(strA30Women);


            //result print (with format)
            context.write(state, spaceValue);

            context.write(new Text("    Aged 0-18  Men"), resultA18Men);
            context.write(new Text("    Aged 19-29 Men"), resultA19Men);
            context.write(new Text("    Aged 30-39 Men"), resultA30Men);


            context.write(new Text("    Aged 0-18  Women"), resultA18Women);
            context.write(new Text("    Aged 19-29 Women"), resultA19Women);
            context.write(new Text("    Aged 30-39 Women"), resultA30Women);
        }

        //Q4:
        context.write(new Text("Q4: Urban vs Rural"), spaceValue);

        for (Map.Entry<String, HashMap<String,Integer>> entry : ans.entrySet()){
            Text state = new Text(entry.getKey());
            HashMap<String ,Integer> tableForOneState = entry.getValue();

            //get variable from table
            Integer urbanHouseholds = tableForOneState.get("urbanHouseholds");
            Integer ruralHouseholds = tableForOneState.get("ruralHouseholds");
            Integer notDefined      = tableForOneState.get("notDefined"     );

            //statistical arithmetics
            Integer totalHouseholds = urbanHouseholds + ruralHouseholds + notDefined;
            Double urbanRate = (urbanHouseholds*1.0  / totalHouseholds )*100;
            Double ruralRate = (ruralHouseholds*1.0  / totalHouseholds) *100;

            //convert to String str
            String strUrban = Double.valueOf(urbanRate)+"%";
            String strRural  = Double.valueOf(ruralRate) + "%";
            Text resultUrban = new Text(strUrban);
            Text resultRural = new Text(strRural);

            //result print (with format)
            context.write(state, spaceValue);
            context.write(new Text("    Urban"), resultUrban);
            context.write(new Text("    Rural"), resultRural);
        }

        //Q5:
        context.write(new Text("Q5: Median value of house occupied by owners"), spaceValue);
        for (Map.Entry<String, HashMap<String,Integer>> entry : ans.entrySet()){
            Text state = new Text(entry.getKey());
            HashMap<String ,Integer> tableForOneState = entry.getValue();

            //get variable from table
            ArrayList<Integer> v = new ArrayList<>();
            int totalOwners = 0;
            for (int i = 0; i < 20; i++) {
                v.add(tableForOneState.get("valueOwnerOccupied" + String.valueOf(i)));
                totalOwners +=v.get(i);
            }

            //statistical arithmetics
            int count = 0;
            int medianIndex = 0;
            for (int i = 0; i < 20; i++){
                count += v.get(i);
                if (count > totalOwners/2)
                {
                    medianIndex = i;
                    break;
                }
            }

            //convert to String str
            ArrayList<String> indexToValue = new ArrayList<>();
            indexToValue.add("Less than $15,000");
            indexToValue.add("$15,000 - $19,999");
            indexToValue.add("$20,000 - $24,999");
            indexToValue.add("$25,000 - $29,999");
            indexToValue.add("$30,000 - $34,999");
            indexToValue.add("$35,000 - $39,999");
            indexToValue.add("$40,000 - $44,999");
            indexToValue.add("$45,000 - $49,999");
            indexToValue.add("$50,000 - $59,999");
            indexToValue.add("$60,000 - $74,999");
            indexToValue.add("$75,000 - $99,999");
            indexToValue.add("$100,000 - $124,999");
            indexToValue.add("$125,000 - $149,999");
            indexToValue.add("$150,000 - $174,999");
            indexToValue.add("$175,000 - $199,999");
            indexToValue.add("$200,000 - $249,999");
            indexToValue.add("$250,000 - $299,999");
            indexToValue.add("$300,000 - $399,999");
            indexToValue.add("$400,000 - $499,999");
            indexToValue.add("$500,000 or more   ");
            Text medianValue = new Text(indexToValue.get(medianIndex));

            //result print (with format)
            context.write(state, spaceValue);
            context.write(new Text("    median value:"), medianValue);

        }

        //Q6
        context.write(new Text("Q6: Median value of rent"), spaceValue);
        for (Map.Entry<String, HashMap<String,Integer>> entry : ans.entrySet()){
            Text state = new Text(entry.getKey());
            HashMap<String ,Integer> tableForOneState = entry.getValue();

            //get variable from table
            ArrayList<Integer> v = new ArrayList<>();
            int totalRent = 0;
            for (int i = 0; i < 16; i++) {
                v.add(tableForOneState.get("valueRent" + String.valueOf(i)));
                totalRent +=v.get(i);
            }

            //statistical arithmetics
            int count = 0;
            int medianIndex = 0;
            for (int i = 0; i < 16; i++){
                count += v.get(i);
                if (count > totalRent/2)
                {
                    medianIndex = i;
                    break;
                }
            }

            //convert to String str
            ArrayList<String> indexToValue = new ArrayList<>();
            indexToValue.add("Less than $100");
            indexToValue.add("$100  to $149 ");
            indexToValue.add("$150  to $199 ");
            indexToValue.add("$200  to $249 ");
            indexToValue.add("$250  to $299 ");
            indexToValue.add("$300  to $349 ");
            indexToValue.add("$350  to $399 ");
            indexToValue.add("$400  to $449 ");
            indexToValue.add("$450  to $499 ");
            indexToValue.add("$500  to $549 ");
            indexToValue.add("$550  to $599 ");
            indexToValue.add("$600  to $649 ");
            indexToValue.add("$650  to $699 ");
            indexToValue.add("$700  to $749 ");
            indexToValue.add("$750  to $999 ");
            indexToValue.add("$1000 or more ");
            Text medianValue = new Text(indexToValue.get(medianIndex));

            //result print (with format)
            context.write(state, spaceValue);
            context.write(new Text("    median rent:"), medianValue);

        }


        //Q7
        context.write(new Text("Q7: 95th percentile of average room number per house"), spaceValue);

        //for certain state (entry of hashmap), calculate total rent house and total room number of all these housese

        ArrayList<StateToAverage> aList = new ArrayList<>();
        for (Map.Entry<String, HashMap<String,Integer>> entry : ans.entrySet()) {
           // Text state = new Text(entry.getKey());
            String state = entry.getKey();
            HashMap<String, Integer> tableForOneState = entry.getValue();

            //get variable from table
            ArrayList<Integer> distribution = new ArrayList<>();
            ArrayList<Integer> weightedPart = new ArrayList<>();

            for (int i = 0; i < 9; i++) {
                distribution.add(tableForOneState.get("distribution" + String.valueOf(i)));
                weightedPart.add(tableForOneState.get("weightedPart" + String.valueOf(i)));
            }

            //total rent house and total rooms
            int totalHouse = 0;
            int totalRooms = 0;
            Double averageRoomNumber = new Double(-1);
            for (int i = 0; i < 9; i++) {
                totalHouse += distribution.get(i);
                totalRooms += weightedPart.get(i);
            }
            if (totalHouse!= 0) {
                averageRoomNumber = totalRooms * 1.0 / totalHouse;
                aList.add(new StateToAverage(state, averageRoomNumber));
            }
        }

        //get sum of average room of each state
        Double sumOfAverage = 0.0;
        for (int i = 0; i < aList.size(); i++)
            sumOfAverage += aList.get(i).getAverage();


        //to calculate the 95th percentile
        Collections.sort(aList);
        Double exceedRoomNumber = Math.ceil(sumOfAverage * 0.95);
        double count = 0.0;
        StateToAverage theOne = new StateToAverage("Initialization", aList.size()+0.33);
        for (int i = 0; i < aList.size();i++) {
            //context.write(new Text("average of State"), new Text(String.valueOf(aList.get(i).getAverage())));
            count+= aList.get(i).getAverage();
            if (count >= exceedRoomNumber){
                theOne = aList.get(i);
                break;
            }
        }
        Text theState = new Text(String.valueOf(theOne.getState()));
        Text pencentile95th = new Text(String.valueOf(theOne.getAverage()));
        //result print (with format)
        context.write(theState, pencentile95th);

        //Q8:
        context.write(new Text("Q8: The state has highest elderly rate"), spaceValue);

        //for certain state (entry of hashmap), calculate total elderly people and state population

        ArrayList<StateToAverage> stateListOfElderlyRate = new ArrayList<>();
        for (Map.Entry<String, HashMap<String,Integer>> entry : ans.entrySet()) {

            String state = entry.getKey();
            HashMap<String, Integer> tableForOneState = entry.getValue();

            //get variable from table
            int elderlyForOneState = tableForOneState.get("elderlyPeople");
            int statePopulation = tableForOneState.get("statePopulation");

            //elderly rate of this state
            Double elderlyRate = new Double(0.0);
            if (statePopulation!= 0) {
                elderlyRate = elderlyForOneState * 1.0 / statePopulation * 100;
                stateListOfElderlyRate.add(new StateToAverage(state, elderlyRate));
            }
        }


        //to calculate the highest elderly rate
        Collections.sort(stateListOfElderlyRate);
        StateToAverage theHighestElderlyRateState = new StateToAverage("Initialization", 0.0);
        theHighestElderlyRateState = stateListOfElderlyRate.get(stateListOfElderlyRate.size()-1);
        Text theHighestState = new Text(String.valueOf(theHighestElderlyRateState.getState()));
        Text highestElderlyRate = new Text(String.valueOf(theHighestElderlyRateState.getAverage())+"%");
        //result print (with format)
        context.write(theHighestState, highestElderlyRate);



        //Q9: print out
        context.write(new Text("Q9:  Index of popularity of rental market for each state"), spaceValue);
        for (Map.Entry<String, HashMap<String,Integer>> entry : ans.entrySet()){
            HashMap<String ,Integer> tableForOneState = entry.getValue();

            //get variable from table
            ArrayList<Integer> duration = new ArrayList<>();
            int totalVacantRentHouses = 0;
            for (int i = 0; i < 3; i++) {
                duration.add(tableForOneState.get("duration" + String.valueOf(i)));
                totalVacantRentHouses +=duration.get(i);
            }

            //statistical arithmetics
            Double indexOfRentMarket = new Double(0.0);
            if (totalVacantRentHouses!=0) {

                indexOfRentMarket = duration.get(0) * 1.0 / totalVacantRentHouses * 100;
                String result_indexOfRentMarket = String.format("%.2f", indexOfRentMarket);

                //convert to String str

                String s = new String("");
                for (int i = 0; i < indexOfRentMarket; i+=4)
                    s = s+"*";
                Text blocks = new Text(s);
                //result print (with format)
                String outKey = entry.getKey() + "  " + result_indexOfRentMarket+"%";
                context.write(new Text(outKey), blocks);
            }
        }









    }
}

