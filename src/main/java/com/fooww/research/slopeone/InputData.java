package com.fooww.research.slopeone;

import com.fooww.research.entity.UserItemScore;
import com.fooww.research.mae.MaeJava;
import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.GenericBooleanPrefDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.slopeone.SlopeOneRecommender;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;

import java.io.*;
import java.util.*;

/**
 * author:zwy
 * Date:2020-01-14
 * Time:22:14
 */
public class InputData {

    public static  Map<Long, Map<Long, Float>> loadFromFile(String file){
        Map<Long,Map<Long,Float>> data = new HashMap<>();
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(file))){
            String line;
            while ((line=bufferedReader.readLine())!=null){
                Long userId = Long.parseLong(line.split(",")[0]) ;
                Long itemId = Long.parseLong(line.split(",")[1]) ;
                Float score = Float.parseFloat(line.split(",")[2]);
                Map<Long,Float> itemScoreMap = data.getOrDefault(userId,new HashMap<>());
                itemScoreMap.put(itemId,score);
                data.put(userId,itemScoreMap);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return data;
    }

    public static void mae(String file){
        Map<Long,Map<Long,Float>> data = loadFromFile(file);

        int currentNumber = 0;
        Map<Long,Map<Long,Float>> trainData = new HashMap<>();
        Map<Long,Map<Long,Float>> testData = new HashMap<>();

        for (Long userId: data.keySet()){
            currentNumber++;
            if (currentNumber%10<8){
                trainData.put(userId,data.get(userId));
            }else {
                testData.put(userId,data.get(userId));
            }
        }

        SlopeOne so = new SlopeOne(trainData);

        List<Float> observe = new ArrayList<>();
        List<Float> predict = new ArrayList<>();

        for (Long userId:testData.keySet()){
            Map<Long,Float> predictData = so.predict(testData.get(userId));
            Map<Long,Float> testUserData = testData.get(userId);

            for (Long itemId:predictData.keySet()){
                if (testUserData.get(itemId)!=null){
                    observe.add(testUserData.get(itemId));
                    predict.add(predictData.get(itemId));
                }
            }
        }

        double mae = MaeJava.getMae(observe,predict);
        System.out.println(mae);
    }

    public static void maeMahout(String file) throws Exception{
        Map<Long,Map<Long,Float>> data = loadFromFile(file);

        int currentNumber = 0;
        Map<Long,Map<Long,Float>> trainData = new HashMap<>();
        Map<Long,Map<Long,Float>> testData = new HashMap<>();

        for (Long userId: data.keySet()){
            currentNumber++;
            if (currentNumber%10<8){
                trainData.put(userId,data.get(userId));
            }else {
                testData.put(userId,data.get(userId));
            }
        }

        DataModel dataModel = loadInMahout(trainData);
        Recommender oneRecommender=new SlopeOneRecommender(dataModel);

        List<Float> observe = new ArrayList<>();
        List<Float> predict = new ArrayList<>();
        testData.forEach((userId,map)-> map.forEach((itemId, score)->{

            try {
                Float predictScore = oneRecommender.estimatePreference(userId,itemId);
                observe.add(score);
                predict.add(predictScore);
            } catch (TasteException e) {
                e.printStackTrace();
            }

        }));

        double mae = MaeJava.getMae(observe,predict);
        System.out.println(mae);
    }

    public static DataModel loadInMahout(Map<Long,Map<Long,Float>> trainData){
        FastByIDMap<PreferenceArray> preMap = new FastByIDMap<>();

        trainData.forEach((userId,itemIdScoreMap)->{
            PreferenceArray preferences = new GenericUserPreferenceArray(itemIdScoreMap.size());
            preferences.setUserID(0,userId);
            int index = 0;
            for (Long itemId : itemIdScoreMap.keySet()){
                preferences.setItemID(index,itemId);
                preferences.setValue(index,itemIdScoreMap.get(itemId));
                index++;
            }
            preMap.put(userId,preferences);

        });

        return new GenericDataModel(preMap);
    }


    public static void main(String[] args) throws Exception{


//        File dataFile=new File("/Users/zhouwenyang/Desktop/郝志远数据/ml-100k.csv");

        maeMahout("/Users/zhouwenyang/Desktop/郝志远数据/ml-100k.csv");
    }

}
