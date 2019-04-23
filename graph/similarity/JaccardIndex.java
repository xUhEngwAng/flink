package com.xun.flink.graph.similarity;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JaccardIndex<K, VV, EV> {
    final private static String path = "D:\\Users\\Documents\\JavaProjects\\similariry\\src\\main\\resources\\graph.txt";
    private static ExecutionEnvironment env;
    public static void main(String[] args) throws Exception{
        JaccardIndex<Integer, NullValue, Integer> jaccardIndex = new JaccardIndex<>();
        env = ExecutionEnvironment.getExecutionEnvironment();

        // construct graph from txt file
        List<Edge<Integer, Integer>> edges = jaccardIndex.readEdgesFromFile(path);
        Graph<Integer, NullValue, Integer> graph = Graph.fromCollection(edges, env);
        graph = graph.getUndirected();

        DataSet<Tuple3<Integer, Integer, Float>> score = jaccardIndex.run(graph);
        score.print();
    }

    public DataSet<Tuple3<K, K, Float>> run(Graph<K, VV, EV> input){

        DataSet<Tuple2<K, Integer>> vertexWithDegree = input
                .getEdges()
                .map(new MapFunction<Edge<K,EV>, Tuple2<K, Integer>>() {
                    @Override
                    public Tuple2<K, Integer> map(Edge<K, EV> edge){
                        return new Tuple2<>(edge.f1, 1);
                    }
                })
                .groupBy(0)
                .reduce((Tuple2<K, Integer> one, Tuple2<K, Integer> two) -> new Tuple2<K, Integer>(one.f0, one.f1 + two.f1));

        DataSet<Tuple3<K, K, Integer>> edgeTargetDegree = input.getEdges()
                .join(vertexWithDegree)
                .where(1)
                .equalTo(0)
                .map(new MapFunction<Tuple2<Edge<K, EV>, Tuple2<K, Integer>>, Tuple3<K, K, Integer>>() {
                    @Override
                    public Tuple3<K, K, Integer> map(Tuple2<Edge<K, EV>, Tuple2<K, Integer>> in){
                        return new Tuple3<>(in.f0.f0, in.f0.f1, in.f1.f1);
                    }
                });

        //generate vertex pairs
        DataSet<Tuple3<K, K, Integer>> vertexPairs = edgeTargetDegree
                .groupBy(0)
                .sortGroup(1, Order.ASCENDING)
                .reduceGroup(new GeneratePairs<K>());

        //Compute Jaccard Index
        DataSet<Tuple3<K, K, Float>> score = vertexPairs
                .groupBy(0, 1)
                .reduceGroup(new GroupReduceFunction<Tuple3<K, K, Integer>, Tuple3<K, K, Float>>() {
                    @Override
                    public void reduce(Iterable<Tuple3<K, K, Integer>> in, Collector<Tuple3<K, K, Float>> out) {
                        int sharedNeighbors = 0;
                        int sumNeighbors = 0;
                        Tuple3<K, K, Float> output = new Tuple3<>();

                        for(Tuple3<K, K, Integer> edge : in) {
                            output.f0 = edge.f0;
                            output.f1 = edge.f1;
                            sumNeighbors = edge.f2;
                            ++sharedNeighbors;
                        }
                        output.f2 = (float)sharedNeighbors / (sumNeighbors - sharedNeighbors);
                        out.collect(output);
                    }
                });

        return score;
    }

    public static class GeneratePairs<K> implements
            GroupReduceFunction<Tuple3<K, K, Integer>, Tuple3<K,K,Integer>>{
        @Override
        public void reduce(Iterable<Tuple3<K, K, Integer>> in, Collector<Tuple3<K, K, Integer>> out){
            List<Tuple3<K, K, Integer>> visited = new ArrayList<>();
            Tuple3<K, K, Integer> prior;
            int count = 0;
            for(Tuple3<K, K, Integer> edge : in){
                for(int ix = 0; ix != count; ++ix){
                    prior = edge.copy();
                    prior.f0 = visited.get(ix).f1;
                    prior.f2 = prior.f2 + visited.get(ix).f2;
                    out.collect(prior);
                }
                ++count;
                visited.add(edge);
            }
        }
    }

    //read from file
    private List<Edge<Integer, Integer>> readEdgesFromFile(String path){
        List<Edge<Integer, Integer>> edges = new ArrayList<>();
        String oneLine;
        Integer[] vertices = new Integer[2];
        try {
            BufferedReader reader = new BufferedReader(new FileReader(path));

            /*for(int i = 0; i != 4; ++i)
                reader.readLine();*/
            while((oneLine = reader.readLine()) != null){
                vertices[0] = Integer.parseInt(oneLine.split(" ")[0]);
                vertices[1] = Integer.parseInt(oneLine.split(" ")[1]);
                edges.add(new Edge<>(vertices[0], vertices[1], 0));
            }

        }catch(IOException ioEx){
            ioEx.printStackTrace();
        }
        return edges;
    }
}
