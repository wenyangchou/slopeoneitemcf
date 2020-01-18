package com.fooww.research.util;

import com.fooww.research.entity.FileEntity;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * author:zwy
 * Date:2020-01-15
 * Time:23:16
 */
public class FileUtil {

    public static FileEntity splitFile(String inputFile,int trainNumber){
        BufferedReader bufferedReader = null;
        BufferedWriter trainFileWriter = null;
        BufferedWriter testFileWriter = null;
        FileEntity fileEntity = new FileEntity();
        try {
             bufferedReader= new BufferedReader(new FileReader(inputFile));
            String trainFile = "train.cvs";
            String testFile = "test.cvs";

            fileEntity.setTestFile(new File(testFile).getAbsolutePath());
            fileEntity.setTrainFile(new File(trainFile).getAbsolutePath());

            trainFileWriter = new BufferedWriter(new FileWriter(trainFile));
            testFileWriter = new BufferedWriter(new FileWriter(testFile));

            String lineStr;
            int lineNumber = 0;
            while ((lineStr=bufferedReader.readLine())!=null){
                if (lineNumber<trainNumber){
                    trainFileWriter.write(lineStr.replaceAll("::", ","));
                    trainFileWriter.newLine();
                }else{
                    testFileWriter.write(lineStr.replaceAll("::", ","));
                    testFileWriter.newLine();
                }
                lineNumber++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                if (bufferedReader != null) {
                    bufferedReader.close();
                }
                if (trainFileWriter != null) {
                    trainFileWriter.close();
                }
                if (testFileWriter != null) {
                    testFileWriter.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return fileEntity;
    }

  public static void main(String[] args) {
      splitFile("C:\\Users\\fooww\\Desktop\\数据\\数据\\ml-100k.csv",80000);
  }

  public static void dealFile(String filePath){
        File file = new File(filePath);
      Map<String,Integer> userIdMap = new HashMap<>();
      Map<String,Integer> itemIdMap = new HashMap<>();
      int currentUserId = 1;
      int currentItemId = 1;

      try(BufferedReader bufferedReader = new BufferedReader(new FileReader(file))){
        String line ;

        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("C:\\Users\\fooww\\Desktop\\数据\\数据\\aa.csv"));

        while ((line = bufferedReader.readLine())!=null){
            String[] lineStrings = line.split(",");
            String userIdStr = lineStrings[0];
            String itemIdStr = lineStrings[1];
            String scoreStr = lineStrings[2];
            String timeStr = lineStrings[3];

            int userId = 0;
            int itemId = 0;

            if (userIdMap.containsKey(userIdStr)){
                userId = userIdMap.get(userIdStr);
            }else {
                userId = ++currentUserId;
                userIdMap.put(userIdStr, userId);
            }

            if (itemIdMap.containsKey(itemIdStr)){
                itemId = itemIdMap.get(itemIdStr);
            }else {
                itemId = ++currentItemId;
                itemIdMap.put(itemIdStr, itemId);
            }

            String outputLineStr = userId+","+itemId+","+scoreStr+","+timeStr;
            bufferedWriter.write(outputLineStr);
            bufferedWriter.newLine();
        }
      } catch (IOException e) {
          e.printStackTrace();
      }


  }
}
