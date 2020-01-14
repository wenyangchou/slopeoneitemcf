package com.fooww.research.slopeone;

import com.fooww.research.mae.MaeJava;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
                Long userId = Long.parseLong(line.split(" ")[0]) ;
                Long itemId = Long.parseLong(line.split(" ")[1]) ;
                Float score = Float.parseFloat(line.split(" ")[2]);
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

}
