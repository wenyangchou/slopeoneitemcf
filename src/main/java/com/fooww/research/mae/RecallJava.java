package com.fooww.research.mae;

import java.util.List;

/**
 * @author ：zwy
 */
public class RecallJava {

    public static double getPrecision(List<Float> observe, List<Float> predict,int testNumber){
        double recallnumber = 0;
        int total = observe.size();
        for (int i = 0; i < observe.size(); i++) {
            if (Math.abs(observe.get(i) - predict.get(i))<=1){
                recallnumber++;
            }
        }
        return recallnumber/testNumber;
    }
}
