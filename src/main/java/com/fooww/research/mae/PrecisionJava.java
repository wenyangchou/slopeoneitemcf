package com.fooww.research.mae;

import java.util.List;

/**
 * @author ：zwy
 */
public class PrecisionJava {

    public static double getPrecision(List<Float> observe, List<Float> predict){
        double precisionNumber = 0;
        int total = observe.size();
        for (int i = 0; i < observe.size(); i++) {
            if (Math.abs(observe.get(i) - predict.get(i))<=1){
                precisionNumber++;
            }
        }
        return precisionNumber/total;
    }
}
