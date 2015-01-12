/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author Deepak
 */
public class kmapper extends Mapper<Object, Text, Text, Text> {

    public static int genecount=0;
    private Text word = new Text();
    public static int iter=0;
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
        String[] split = value.toString().split(" ");
        Integer gid = Integer.parseInt(split[0]);
        List<Double> newexp = new ArrayList<Double>();
        StringBuilder expstr=new StringBuilder();
        for (int i = 2; i < split.length; i++) {
            newexp.add(Double.parseDouble(split[i]));
            expstr.append(split[i]);
            expstr.append(" ");
        }
        Configuration jc = context.getConfiguration();
        jc.set("g"+gid.toString(), expstr.toString());
        jc.set("genecount", String.valueOf(++genecount));
        String[] cexpstr = new String[5];
        cexpstr[0] = jc.get("1");
        cexpstr[1] = jc.get("2");
        cexpstr[2] = jc.get("3");
        cexpstr[3] = jc.get("4");
        cexpstr[4] = jc.get("5");
        Map<Integer, List<Double>> cmap = new HashMap<Integer, List<Double>>();
        for (int i = 0; i < 5; i++) {
            Integer cid = i+1;
            String[] temp = cexpstr[i].split(" ");
            List<Double> exptemp = new ArrayList<Double>();
            for (int k = 0; k < temp.length; k++) {
                exptemp.add(Double.parseDouble(temp[k]));
            }
            cmap.put(cid, exptemp);
        }
        List<Double> geneExpressionValues = new ArrayList<Double>();
        Double minDist = Double.MAX_VALUE;
        Integer minDistIndex = 0;
        geneExpressionValues.addAll(newexp);
        for (int j = 0; j < cmap.size(); j++) {
            double eucDistance = 0.0;
            List<Double> centroidExpressionValues = new ArrayList<Double>();
            centroidExpressionValues = cmap.get(j);
            for (int i = 0; i < centroidExpressionValues.size(); i++) {
                double dist = geneExpressionValues.get(i) - centroidExpressionValues.get(i);
                eucDistance += Math.pow(dist, 2);
            }
            double dist = Math.sqrt(eucDistance);
            if (dist < minDist) {
                minDist = dist;
                minDistIndex = j;
            }
        }
        word.set(gid.toString()+":"+String.valueOf(minDistIndex+1));
        context.write(new Text(" "), word);
        
    }

}
