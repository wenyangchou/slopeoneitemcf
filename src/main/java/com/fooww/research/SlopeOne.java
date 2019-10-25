package com.fooww.research;

import java.util.*;

/** @author ：zwy */
public class SlopeOne {

  Map<String, Map<String, Integer>> frequency = null;
  Map<String, Map<String, Double>> deviation = null;
  Map<String, Map<String, Integer>> user_rating = null;

  public SlopeOne(Map<String, Map<String, Integer>> user_rating) {
    frequency = new HashMap<>();
    deviation = new HashMap<>();
    this.user_rating = user_rating;
  }

  public static void main(String[] args) {
    Map<String, Map<String, Integer>> userRatings = new HashMap<>();
    Map<String, Integer> xiMingRating = new HashMap<>();
    xiMingRating.put("张学友", 4);
    xiMingRating.put("周杰伦", 3);
    xiMingRating.put("刘德华", 4);
    Map<String, Integer> xiHaiRating = new HashMap<>();
    xiHaiRating.put("张学友", 5);
    xiHaiRating.put("周杰伦", 2);
    Map<String, Integer> liMeiRating = new HashMap<>();
    liMeiRating.put("周杰伦", 3);
    liMeiRating.put("刘德华", 4);
    Map<String, Integer> liLeiRating = new HashMap<>();
    liLeiRating.put("张学友", 5);
    liLeiRating.put("刘德华", 3);
    userRatings.put("xiMing", xiMingRating);
    userRatings.put("xiHai", xiHaiRating);
    userRatings.put("liMei", liMeiRating);
    userRatings.put("liLei", liLeiRating);

    SlopeOne slopOne = new SlopeOne(userRatings);
    slopOne.computeDeviation();
    List<Map.Entry<String, Double>> top_k = slopOne.predictRating(userRatings.get("liLei"), 5);
    for (Map.Entry<String, Double> item : top_k) {
      System.out.println(item.getKey() + "   " + item.getValue());
    }
  }

  /** 所有有item间的评分偏差 */
  public void computeDeviation() {
    for (Map.Entry<String, Map<String, Integer>> ratingsEntry : user_rating.entrySet()) {
      for (Map.Entry<String, Integer> ratingEntry : ratingsEntry.getValue().entrySet()) {
        String item = ratingEntry.getKey();
        int rating = ratingEntry.getValue();
        Map<String, Integer> itemFrequency = null;
        if (!frequency.containsKey(item)) {
          itemFrequency = new HashMap<>();
          frequency.put(item, itemFrequency);
        } else {
          itemFrequency = frequency.get(item);
        }

        Map<String, Double> itemDeviation = null;
        if (!deviation.containsKey(item)) {
          itemDeviation = new HashMap<>();
          deviation.put(item, itemDeviation);
        } else {
          itemDeviation = deviation.get(item);
        }

        for (Map.Entry<String, Integer> ratingEntry2 : ratingsEntry.getValue().entrySet()) {
          String item2 = ratingEntry2.getKey();
          int rating2 = ratingEntry2.getValue();
          if (!item.equals(item2)) {
            // 两个项目的用户数
            itemFrequency.put(
                item2, itemFrequency.containsKey(item2) ? itemFrequency.get(item2) + 1 : 0);
            // 两个项目的评分偏差，累加
            itemDeviation.put(
                item2,
                itemDeviation.containsKey(item2)
                    ? itemDeviation.get(item2) + (rating - rating2)
                    : 0.0);
          }
        }
      }
    }

    for (Map.Entry<String, Map<String, Double>> itemsDeviation : deviation.entrySet()) {
      String item = itemsDeviation.getKey();
      Map<String, Double> itemDev = itemsDeviation.getValue();
      Map<String, Integer> itemFre = frequency.get(item);
      for (String itemName : itemDev.keySet()) {
        itemDev.put(itemName, itemDev.get(itemName) / itemFre.get(itemName));
      }
    }
  }

  /**
   * 评分预测
   *
   * @param userRating 目标用户的评分
   * @param k 返回前k个
   * @return
   */
  public List<Map.Entry<String, Double>> predictRating(Map<String, Integer> userRating, int k) {
    Map<String, Double> recommendations = new HashMap<>();
    Map<String, Integer> frequencies = new HashMap<>();
    for (Map.Entry<String, Integer> userEntry : userRating.entrySet()) {
      String userItem = userEntry.getKey();
      double rating = userEntry.getValue();
      for (Map.Entry<String, Map<String, Double>> deviationEntry : deviation.entrySet()) {
        String item = deviationEntry.getKey();
        Map<String, Double> itemDeviation = deviationEntry.getValue();
        Map<String, Integer> itemFrequency = frequency.get(item);
        if (!userRating.containsKey(item) && itemDeviation.containsKey(userItem)) {
          int fre = itemFrequency.get(userItem);
          if (!recommendations.containsKey(item)) recommendations.put(item, 0.0);
          if (!frequencies.containsKey(item)) frequencies.put(item, 0);
          // 分子部分
          recommendations.put(
              item, recommendations.get(item) + (itemDeviation.get(userItem) + rating) * fre);
          // 分母部分
          frequencies.put(item, frequencies.get(item) + fre);
        }
      }
    }
    for (Map.Entry<String, Double> recoEntry : recommendations.entrySet()) {
      String key = recoEntry.getKey();
      double value = recoEntry.getValue() / frequencies.get(key);
      recommendations.put(key, value);
    }
    // 排序，这里还可以使用优先队列返回top_k
    List<Map.Entry<String, Double>> list_map =
        new ArrayList<>(recommendations.entrySet());
    list_map.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));
    List<Map.Entry<String, Double>> top_k = new ArrayList<>();
    if (list_map.size() < k) k = list_map.size();
    for (int i = 0; i < k; i++) {
      top_k.add(list_map.get(i));
    }
    return top_k;
  }
}
