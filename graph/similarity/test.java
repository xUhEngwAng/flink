package com.xun.flink.graph.similarity;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.similarity.JaccardIndex;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.NullValue;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class test {
    final static String path = "D:\\Users\\Documents\\JavaProjects\\similariry\\src\\main\\resources\\0.txt";

    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Edge<IntValue, Integer>> edges = new test().readEdgesFromFile(path);
        Graph<IntValue, NullValue, Integer> graph = Graph.fromCollection(edges, env);
        graph = graph.getUndirected();

        long begin = System.currentTimeMillis();
        DataSet<JaccardIndex.Result<IntValue>> scores = new JaccardIndex().runInternal(graph);
        //scores.print();
        long end = System.currentTimeMillis();
        System.out.println(end - begin);
    }

    //read from file
    private List<Edge<IntValue, Integer>> readEdgesFromFile(String path) {
        List<Edge<IntValue, Integer>> edges = new ArrayList<>();
        String oneLine;
        Integer[] vertices = new Integer[2];
        try {
            BufferedReader reader = new BufferedReader(new FileReader(path));

        /*for(int i = 0; i != 4; ++i)
            reader.readLine();*/
            while ((oneLine = reader.readLine()) != null) {
                vertices[0] = Integer.parseInt(oneLine.split(" ")[0]);
                vertices[1] = Integer.parseInt(oneLine.split(" ")[1]);
                edges.add(new Edge<>(new IntValue(vertices[0]), new IntValue(vertices[1]), 1));
            }

        } catch (IOException ioEx) {
            ioEx.printStackTrace();
        }
        return edges;
    }
}
