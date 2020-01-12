package com.fooww.research.slopeone;


/**
 * author:zwy
 * Date:2020-01-05
 * Time:15:10
 */

import java.util.*;


public class SlopeOne {

    Map<Long, Map<Long, Float>> mData;
    Map<Long, Map<Long, Float>> mDiffMatrix;
    Map<Long, Map<Long, Integer>> mFreqMatrix;

    static Long[] mAllItems;

    public SlopeOne(Map<Long, Map<Long, Float>> data) {
        mData = data;
        buildDiffMatrix();
    }

    /**
     * Based on existing data, and using weights, try to predict all missing
     * ratings. The trick to make this more scalable is to consider only
     * mDiffMatrix entries having a large (>1) mFreqMatrix entry.
     *
     * It will output the prediction 0 when no prediction is possible.
     */
    public Map<Long, Float> predict(Map<Long, Float> user) {
        HashMap<Long, Float> predictions = new HashMap<Long, Float>();
        HashMap<Long, Integer> frequencies = new HashMap<Long, Integer>();
        for (Long j : mDiffMatrix.keySet()) {
            frequencies.put(j, 0);
            predictions.put(j, 0.0f);
        }
        for (Long j : user.keySet()) {
            for (Long k : mDiffMatrix.keySet()) {
                try {
                    float newval = (mDiffMatrix.get(k).get(j).floatValue() + user.get(j)
                            .floatValue()) * mFreqMatrix.get(k).get(j).intValue();
                    predictions.put(k, predictions.get(k) + newval);
                    frequencies.put(k, frequencies.get(k)
                            + mFreqMatrix.get(k).get(j).intValue());
                } catch (NullPointerException e) {
                }
            }
        }
        HashMap<Long, Float> cleanpredictions = new HashMap<Long, Float>();
        for (Long j : predictions.keySet()) {
            if (frequencies.get(j) > 0) {
                cleanpredictions.put(j, predictions.get(j).floatValue()
                        / frequencies.get(j).intValue());
            }
        }
        for (Long j : user.keySet()) {
            cleanpredictions.put(j, user.get(j));
        }
        return cleanpredictions;
    }

    /**
     * Based on existing data, and not using weights, try to predict all missing
     * ratings. The trick to make this more scalable is to consider only
     * mDiffMatrix entries having a large (>1) mFreqMatrix entry.
     */
    public Map<Long, Float> weightlesspredict(Map<Long, Float> user) {
        HashMap<Long, Float> predictions = new HashMap<Long, Float>();
        HashMap<Long, Integer> frequencies = new HashMap<Long, Integer>();
        for (Long j : mDiffMatrix.keySet()) {
            predictions.put(j, 0.0f);
            frequencies.put(j, 0);
        }
        for (Long j : user.keySet()) {
            for (Long k : mDiffMatrix.keySet()) {
                // System.out.println("Average diff between "+j+" and "+ k +
                // " is "+mDiffMatrix.get(k).get(j).floatValue()+" with n = "+mFreqMatrix.get(k).get(j).floatValue());
                float newval = (mDiffMatrix.get(k).get(j).floatValue() + user.get(j)
                        .floatValue());
                predictions.put(k, predictions.get(k) + newval);
            }
        }
        for (Long j : predictions.keySet()) {
            predictions.put(j, predictions.get(j).floatValue() / user.size());
        }
        for (Long j : user.keySet()) {
            predictions.put(j, user.get(j));
        }
        return predictions;
    }

    public void printData() {
        for (Long user : mData.keySet()) {
            System.out.println(user);
            print(mData.get(user));
        }
        for (int i = 0; i < mAllItems.length; i++) {
            System.out.print("\n" + mAllItems[i] + ":");
            printMatrixes(mDiffMatrix.get(mAllItems[i]),
                    mFreqMatrix.get(mAllItems[i]));
        }
    }

    private void printMatrixes(Map<Long, Float> ratings,
                               Map<Long, Integer> frequencies) {
        for (int j = 0; j < mAllItems.length; j++) {
            System.out.format("%10.3f", ratings.get(mAllItems[j]));
            System.out.print(" ");
            System.out.format("%10d", frequencies.get(mAllItems[j]));
        }
        System.out.println();
    }

    public static void print(Map<Long, Float> user) {
        for (Long j : user.keySet()) {
            System.out.println(" " + j + " --> " + user.get(j).floatValue());
        }
    }

    public void buildDiffMatrix() {
        mDiffMatrix = new HashMap<Long, Map<Long, Float>>();
        mFreqMatrix = new HashMap<Long, Map<Long, Integer>>();
        // first iterate through users
        for (Map<Long, Float> user : mData.values()) {
            // then iterate through user data
            for (Map.Entry<Long, Float> entry : user.entrySet()) {
                if (!mDiffMatrix.containsKey(entry.getKey())) {
                    mDiffMatrix.put(entry.getKey(), new HashMap<Long, Float>());
                    mFreqMatrix.put(entry.getKey(), new HashMap<Long, Integer>());
                }
                for (Map.Entry<Long, Float> entry2 : user.entrySet()) {
                    int oldcount = 0;
                    if (mFreqMatrix.get(entry.getKey()).containsKey(entry2.getKey()))
                        oldcount = mFreqMatrix.get(entry.getKey()).get(entry2.getKey())
                                .intValue();
                    float olddiff = 0.0f;
                    if (mDiffMatrix.get(entry.getKey()).containsKey(entry2.getKey()))
                        olddiff = mDiffMatrix.get(entry.getKey()).get(entry2.getKey())
                                .floatValue();
                    float observeddiff = entry.getValue() - entry2.getValue();
                    mFreqMatrix.get(entry.getKey()).put(entry2.getKey(), oldcount + 1);
                    mDiffMatrix.get(entry.getKey()).put(entry2.getKey(),
                            olddiff + observeddiff);
                }
            }
        }
        for (Long j : mDiffMatrix.keySet()) {
            for (Long i : mDiffMatrix.get(j).keySet()) {
                float oldvalue = mDiffMatrix.get(j).get(i).floatValue();
                int count = mFreqMatrix.get(j).get(i).intValue();
                mDiffMatrix.get(j).put(i, oldvalue / count);
            }
        }
    }


    public static void main(String[] args) {

        Map<Long, Map<Long, Float>> data = new HashMap<>();
        // items
        Long item1 = 1L;
        Long item2 = 2L;
        Long item3 = 3L;
        Long item4 = 4L;
        Long item5 = 5L;

        mAllItems = new Long[] { item1, item2, item3, item4, item5 };

        // I'm going to fill it in
        HashMap<Long, Float> user1 = new HashMap<>();
        HashMap<Long, Float> user2 = new HashMap<>();
        HashMap<Long, Float> user3 = new HashMap<>();
        HashMap<Long, Float> user4 = new HashMap<>();
        user1.put(item1, 1.0f);
        user1.put(item2, 0.5f);
        user1.put(item4, 0.1f);
        data.put(1L, user1);
        user2.put(item1, 1.0f);
        user2.put(item3, 0.5f);
        user2.put(item4, 0.2f);
        data.put(2L, user2);
        user3.put(item1, 0.9f);
        user3.put(item2, 0.4f);
        user3.put(item3, 0.5f);
        user3.put(item4, 0.1f);
        data.put(3L, user3);
        user4.put(item1, 0.1f);
        // user4.put(item2,0.4f);
        // user4.put(item3,0.5f);
        user4.put(item4, 1.0f);
        user4.put(item5, 0.4f);
        data.put(4L, user4);
        // next, I create my predictor engine
        SlopeOne so = new SlopeOne(data);
        System.out.println("Here's the data I have accumulated...");
        so.printData();
        // then, I'm going to test it out...
        HashMap<Long, Float> user = new HashMap<>();
        System.out.println("Ok, now we predict...");
        user.put(item5, 0.4f);
        System.out.println("Inputting...");
        SlopeOne.print(user);
        System.out.println("Getting...");
        SlopeOne.print(so.predict(user));
        //
        user.put(item4, 0.2f);
        System.out.println("Inputting...");
        SlopeOne.print(user);
        System.out.println("Getting...");
        SlopeOne.print(so.predict(user));
    }
}

