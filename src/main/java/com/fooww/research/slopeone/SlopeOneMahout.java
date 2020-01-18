package com.fooww.research.slopeone;

import com.fooww.research.entity.FileEntity;
import com.fooww.research.entity.UserItemScore;
import com.fooww.research.mae.MaeJava;
import com.fooww.research.util.FileUtil;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.slopeone.SlopeOneRecommender;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.fooww.research.slopeone.InputData.loadItemFromFile;

/**
 * author:zwy
 * Date:2020-01-18
 * Time:12:42
 */
public class SlopeOneMahout {

    public static void maeMahout(String file,int trainNumber) {

        try {
            FileEntity fileEntity = FileUtil.splitFile(file,trainNumber);
            String trainFile = fileEntity.getTrainFile();
            DataModel dataModel = new FileDataModel(new File(trainFile));
            Recommender oneRecommender=new SlopeOneRecommender(dataModel);
            List<UserItemScore> userItemScores = loadItemFromFile(file);

            List<Float> observe = new ArrayList<>();
            List<Float> predict = new ArrayList<>();
            userItemScores.forEach(userItemScore -> {
                try {
                    Float observeScore = userItemScore.getScore();
                    Float predictScore = oneRecommender.estimatePreference(userItemScore.getUser(),userItemScore.getItem());
                    if (!predictScore.isNaN()){
                        observe.add(observeScore);
                        predict.add(predictScore);
                    }

                } catch (TasteException e) {
                    e.printStackTrace();
                }
            });

            Double mae = MaeJava.getMae(observe,predict);
            System.out.println(mae);
        } catch (IOException | TasteException e) {
            System.out.println("no this user,pass");
        }
    }

    public static void main(String[] args) {
        maeMahout("/Users/zhouwenyang/Desktop/郝志远数据/ml-100k.csv",60000);
    }
}
