package com.fooww.research.util;

import com.fooww.research.entity.FileEntity;

import java.io.*;

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
                    trainFileWriter.write(lineStr);
                    trainFileWriter.newLine();
                }else{
                    testFileWriter.write(lineStr);
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
}
