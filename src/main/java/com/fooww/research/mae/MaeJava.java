package com.fooww.research.mae;

/**
 * author:zwy
 * Date:2020-01-12
 * Time:16:27
 */
public class MaeJava {

    public double getMae(int[] observe,int[] predict){
        if (observe.length!=predict.length){
            return -1D;
        }

        int totalNumber = observe.length;
        double sumMae = 0D;

        for (int i = 0; i < observe.length; i++) {
            sumMae += Math.abs(observe[i] - predict[i])/totalNumber;
        }
        return sumMae;
    }

    public double getMae(float[] observe,float[] predict){
        if (observe.length!=predict.length){
            return -1D;
        }

        int totalNumber = observe.length;
        double sumMae = 0D;

        for (int i = 0; i < observe.length; i++) {
            sumMae += Math.abs(observe[i] - predict[i])/totalNumber;
        }
        return sumMae;
    }
}
