package com.fooww.research.cf;

import com.fooww.research.entity.FileEntity;
import com.fooww.research.entity.UserItemScore;
import com.fooww.research.mae.MaeJava;
import com.fooww.research.util.FileUtil;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.AbstractRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.CustomPearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.EuclideanDistanceSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.fooww.research.slopeone.InputData.loadItemFromFile;

/**
 * author:zwy
 * Date:2020-01-18
 * Time:12:34
 */
public class ItemCFMahout {


    public static void maeItemCF(String file,int trainNumber){
        try {
            FileEntity fileEntity = FileUtil.splitFile(file,trainNumber);
            String trainFile = fileEntity.getTrainFile();
            DataModel dataModel = new FileDataModel(new File(trainFile));
            ItemSimilarity itemSimilarity = new CustomPearsonCorrelationSimilarity(dataModel,0.001);

            Recommender recommender = new GenericItemBasedRecommender(dataModel,itemSimilarity);
            List<UserItemScore> userItemScores = loadItemFromFile(file);

            List<Float> observe = new ArrayList<>();
            List<Float> predict = new ArrayList<>();
            userItemScores.forEach(userItemScore -> {
                try {
                    Float observeScore = userItemScore.getScore();
                    Float predictScore = recommender.estimatePreference(userItemScore.getUser(),userItemScore.getItem());

                    if (!predictScore.isNaN()){
                        observe.add(observeScore);
                        predict.add(predictScore);
                    }

                } catch (TasteException e) {
                    System.out.println("no this user ,pass");
                }
            });
            Double mae = MaeJava.getMae(observe,predict);
            System.out.println(mae);
        } catch (IOException | TasteException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        maeItemCF("/Users/zhouwenyang/Desktop/郝志远数据/ml-100k.csv",60000);
    }
}
