/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dm_p2_mr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import org.apache.hadoop.fs.FileSystem;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 *
 * @author Deepak
 */
public class DM_P2_MR {

    /**
     * @param args the command line arguments
     */
    static Map<Integer, List<Double>> linkedHashMap = new LinkedHashMap<Integer, List<Double>>();
    static List<Double> expressionValues;
    static TreeMap<Integer, Integer> geneToCluster = new TreeMap<>();
    static TreeMap<Integer, Integer> validationMap = new TreeMap<>();
    public static int iter = 0;
    public static String[] init_centroids={"1","7","9","10","15"};

    public static void generateLinkedHashMap(String filename) {
        String filePath = new File("").getAbsolutePath();
        try {
            Scanner s = new Scanner(new File("input/" + filename));
            while (s.hasNext()) {
                String inputLine = s.nextLine();
                String[] splitData = inputLine.split("\t");
                int geneId = Integer.parseInt(splitData[0]);
                int valgeneId = Integer.parseInt(splitData[1]);
                expressionValues = new ArrayList<Double>();
                for (int i = 2; i < splitData.length; i++) {
                    expressionValues.add(Double.parseDouble(splitData[i]));
                }
                linkedHashMap.put(geneId, expressionValues);
                if (valgeneId != -1) {
                    validationMap.put(geneId, valgeneId);
                }
            }
            //System.out.println(linkedHashMap.entrySet());
            //System.out.println(validationMap.entrySet());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception{
        // TODO code application logic here
        generateLinkedHashMap("cho.txt");
        while (iter < 10) {
            if (iter == 0) {
                Configuration confg = new Configuration();
                
                for (int i = 0; i < init_centroids.length; i++) {
                    List<Double> exps = linkedHashMap.get(Integer.parseInt(init_centroids[i]));
                    StringBuilder temp = new StringBuilder();
                    for (int k = 0; k < exps.size(); k++) {
                        temp.append(exps.get(i));
                        temp.append(" ");
                    }
                    confg.set(String.valueOf(i + 1), temp.toString());

                }
                
                Job job = Job.getInstance(confg);
                job.setJobName("mapreduce");

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                job.setMapperClass(kmapper.class);
                //conf.setCombinerClass(Reduce.class);
                job.setReducerClass(kreducer.class);
//                FileInputFormat.addInputPath(job, new Path(inputPath));
//                FileOutputFormat.setOutputPath(job, new Path(outputPath));
//                
//                job.setInputFormat(TextInputFormat.class);
//                conf.setOutputFormat(TextOutputFormat.class);
                String filePath = new File("").getAbsolutePath();
                String inputPath="/input";
                String outputPath="/output";
                FileInputFormat.setInputPaths(job, new Path(inputPath));
                FileOutputFormat.setOutputPath(job, new Path(outputPath));
//                for (int i = 0; i < init_centroids.length; i++) {
//                    List<Double> exps = linkedHashMap.get(Integer.parseInt(init_centroids[i]));
//                    StringBuilder temp = new StringBuilder();
//                    for (int k = 0; k < exps.size(); k++) {
//                        temp.append(exps.get(i));
//                        temp.append(" ");
//                    }
//                    conf.set(init_centroids[i], temp.toString());
//
//                }
                job.waitForCompletion(true);
                //JobClient.runJob(job);
            } else {
                Configuration confg = new Configuration();
                FileSystem fOpen = FileSystem.get(confg);
                Path outputPathReduceFile = new Path("/output/part-r-00000");
                BufferedReader reader = new BufferedReader(new InputStreamReader(fOpen.open(outputPathReduceFile)));
                String Line=reader.readLine();
                while(Line!=null){
                    String[] split=Line.split(":");
                    confg.set(split[0], split[1]);
                    Line=reader.readLine();
                }

//                for (int i = 0; i < init_centroids.length; i++) {
//                    List<Double> exps = linkedHashMap.get(Integer.parseInt(init_centroids[i]));
//                    StringBuilder temp = new StringBuilder();
//                    for (int k = 0; k < exps.size(); k++) {
//                        temp.append(exps.get(i));
//                        temp.append(" ");
//                    }
//                    confg.set(String.valueOf(i + 1), temp.toString());
//
//                }
                
                Job job = Job.getInstance(confg);
                job.setJobName("mapreduce");

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);

                job.setMapperClass(kmapper.class);
                //conf.setCombinerClass(Reduce.class);
                job.setReducerClass(kreducer.class);
//                FileInputFormat.addInputPath(job, new Path(inputPath));
//                FileOutputFormat.setOutputPath(job, new Path(outputPath));
//                
//                job.setInputFormat(TextInputFormat.class);
//                conf.setOutputFormat(TextOutputFormat.class);
                String filePath = new File("").getAbsolutePath();
                String inputPath="/input";
                String outputPath="/output";
                FileInputFormat.setInputPaths(job, new Path(inputPath));
                FileOutputFormat.setOutputPath(job, new Path(outputPath));
//                for (int i = 0; i < init_centroids.length; i++) {
//                    List<Double> exps = linkedHashMap.get(Integer.parseInt(init_centroids[i]));
//                    StringBuilder temp = new StringBuilder();
//                    for (int k = 0; k < exps.size(); k++) {
//                        temp.append(exps.get(i));
//                        temp.append(" ");
//                    }
//                    conf.set(init_centroids[i], temp.toString());
//
//                }
                job.waitForCompletion(true);
                //JobClient.runJob(job);
            }
            iter++;
        }
    }

}
