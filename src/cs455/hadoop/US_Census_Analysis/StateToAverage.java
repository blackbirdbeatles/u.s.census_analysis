package cs455.hadoop.US_Census_Analysis;

/**
 * Created by MyGarden on 4/12/17.
 */
public class StateToAverage implements Comparable<StateToAverage> {
    private String state;
    private Double average;

    public StateToAverage(String state, Double average){
        this.state =state;
        this.average =average;
    }

    public String getState() {
        return state;
    }

    public Double getAverage() {
        return average;
    }

    public int compareTo(StateToAverage sta) {
        if (this.average == sta.getAverage())
            return 0;
        else
            return this.average > sta.getAverage()? 1: -1;
    }


}
