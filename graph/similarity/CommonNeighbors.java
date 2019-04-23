package com.xun.flink.graph.similarity;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
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

//without grouping: 6 seconds approximately
//using grouping & parallelism: 6 seconds still...
public class CommonNeighbors<K, VV, EV> {
    final private static String path = "D:\\Users\\Documents\\JavaProjects\\similariry\\src\\main\\resources\\0.txt";
    final static int DEFAULT_GROUP_SIZE = 64;
    final static int DEFAULT_PARALLELISM = 2;

    private static int groupSize;
    private static int parallelism;

    CommonNeighbors(){
        groupSize = DEFAULT_GROUP_SIZE;
        parallelism = DEFAULT_PARALLELISM;
    }

    public static void main(String[] args) throws Exception {
        CommonNeighbors<Integer, NullValue, Integer> commonNeighbors = new CommonNeighbors<>();
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // construct graph from txt file
        List<Edge<Integer, Integer>> edges = commonNeighbors.readEdgesFromFile(path);
        Graph<Integer, NullValue, Integer> graph = Graph.fromCollection(edges, env);
        graph = graph.getUndirected();

        long start = System.currentTimeMillis();
        DataSet<Tuple3<Integer, Integer, Integer>> scores = commonNeighbors.run(graph);
        //scores.print();
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }

    public DataSet<Tuple3<K, K, Integer>> run(Graph<K, VV, EV> input){

        DataSet<Tuple3<Integer, K, K>> groupSpans = input
                .getEdges()
                .groupBy(0)
                .sortGroup(1, Order.ASCENDING)
                .reduceGroup(new GenerateGroupSpans<>())
                .setParallelism(parallelism);

        DataSet<Tuple3<Integer, K, K>> groups = groupSpans
                .rebalance()
                .setParallelism(parallelism)
                .flatMap(new GenerateGroups<>())
                .setParallelism(parallelism);

        DataSet<Tuple2<K, K>> vertexPairs = groups
                .groupBy(0, 1)
                .sortGroup(2, Order.ASCENDING)
                .reduceGroup(new GeneratePairs<K>());

        DataSet<Tuple3<K, K, Integer>> score = vertexPairs
                .groupBy(0, 1)
                .reduceGroup(new GroupReduceFunction<Tuple2<K, K>, Tuple3<K, K, Integer>>() {
                    @Override
                    public void reduce(Iterable<Tuple2<K, K>> in, Collector<Tuple3<K, K, Integer>> out) throws Exception {
                        Tuple3<K, K, Integer> output = new Tuple3<>();
                        int count = 0;
                        for(Tuple2<K, K> edge : in) {
                            output.f0 = edge.f0;
                            output.f1 = edge.f1;
                            ++count;
                        }
                        output.f2 = count;
                        out.collect(output);
                    }
                });
        return score;
    }

    private static class GenerateGroups<K> implements
            FlatMapFunction<Tuple3<Integer, K, K>, Tuple3<Integer, K, K>>{
        @Override
        public void flatMap(Tuple3<Integer, K, K> in, Collector<Tuple3<Integer, K, K>> out){
            int currSpan = in.f0;
            for(int count = 1; count <= currSpan; ++count){
                out.collect(new Tuple3<>(count, in.f1, in.f2));
            }
        }
    }

    private static class GenerateGroupSpans<K, EV> implements
            GroupReduceFunction<Edge<K, EV>, Tuple3<Integer, K, K>>{
        @Override
        public void reduce(Iterable<Edge<K, EV>> in, Collector<Tuple3<Integer, K, K>> out){
            int groupSpan = 1;
            int count = 0;
            for(Edge<K, EV> edge : in){
                if(count++ == groupSize){
                    ++groupSpan;
                    count = 0;
                }
                out.collect(new Tuple3<>(groupSpan, edge.f0, edge.f1));
            }
        }
    }

    private static class GeneratePairs<K> implements
            GroupReduceFunction<Tuple3<Integer, K, K>, Tuple2<K, K>> {
        @Override
        public void reduce(Iterable<Tuple3<Integer, K, K>> in, Collector<Tuple2<K, K>> out){
            List<Tuple3<Integer, K, K>> visited = new ArrayList<>();
            Tuple2<K, K> prior;
            int count = 0;
            for(Tuple3<Integer, K, K> edge : in){
                for(int ix = 0; ix != count; ++ix){
                    prior = new Tuple2<>(visited.get(ix).f2, edge.f2);
                    out.collect(prior);
                }
                if(count < groupSize) {
                    ++count;
                    visited.add(edge);
                }
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
                edges.add(new Edge<>(vertices[0], vertices[1], 1));
            }

        }catch(IOException ioEx){
            ioEx.printStackTrace();
        }
        return edges;
    }
}
