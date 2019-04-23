package com.xun.flink.graph.similarity;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.undirected.VertexDegree;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

//without grouping: 6 seconds approximately
//using grouping & parallelism: 6 seconds still...
public class ResourcesAllocation<K, VV, EV> {
    final private static String path = "D:\\Users\\Documents\\JavaProjects\\similariry\\src\\main\\resources\\0.txt";
    final static int DEFAULT_GROUP_SIZE = 64;
    final static int DEFAULT_PARALLELISM = -1;

    private static int groupSize;
    private static int parallelism;

    ResourcesAllocation(){
        groupSize = DEFAULT_GROUP_SIZE;
        parallelism = DEFAULT_PARALLELISM;
    }

    public static void main(String[] args) throws Exception {
        ResourcesAllocation<Integer, NullValue, Integer> pa = new ResourcesAllocation<>();
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // construct graph from txt file
        List<Edge<Integer, Integer>> edges = pa.readEdgesFromFile(path);
        Graph<Integer, NullValue, Integer> graph = Graph.fromCollection(edges, env);
        graph = graph.getUndirected();

        long start = System.currentTimeMillis();
        DataSet<Tuple3<Integer, Integer, Float>> scores = pa.run(graph);
        //scores.print();
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }

    public void setGroupSize(int size){
         groupSize = size;
    }

    public void setParallelism(int paral){
        parallelism = paral;
    }

    public DataSet<Tuple3<K, K, Float>> run(Graph<K, VV, EV> input) throws Exception{
        DataSet<Tuple2<K, Float>> inverseDegree = input
                .run(new VertexDegree<K, VV, EV>())
                .map(new MapFunction<Vertex<K, LongValue>, Tuple2<K, Float>>() {
                    @Override
                    public Tuple2<K, Float> map(Vertex<K, LongValue> in){
                        return new Tuple2<K, Float>(in.f0, 1f/in.f1.getValue());
                    }
                })
                .setParallelism(parallelism);

        DataSet<Tuple3<K, K, Float>> srcInverseDegree = input
                .getEdges()
                .join(inverseDegree, JoinOperatorBase.JoinHint.REPARTITION_HASH_SECOND)
                .where(0)
                .equalTo(0)
                .map(new MapFunction<Tuple2<Edge<K, EV>, Tuple2<K, Float>>, Tuple3<K, K, Float>>() {
                    @Override
                    public Tuple3<K, K, Float> map(Tuple2<Edge<K, EV>, Tuple2<K, Float>> in){
                        return new Tuple3<>(in.f0.f0, in.f0.f1, in.f1.f1);
                    }
                })
                .setParallelism(parallelism);

        DataSet<Tuple4<Integer, K, K, Float>> groupSpans = srcInverseDegree
                .groupBy(0)
                .sortGroup(1, Order.ASCENDING)
                .reduceGroup(new GenerateGroupSpans<>())
                .setParallelism(parallelism);

        DataSet<Tuple4<Integer, K, K, Float>> groups = groupSpans
                .rebalance()
                .setParallelism(parallelism)
                .flatMap(new GenerateGroups<>())
                .setParallelism(parallelism);

        DataSet<Tuple3<K, K, Float>> vertexPairs = groups
                .groupBy(0, 1)
                .sortGroup(2, Order.ASCENDING)
                .reduceGroup(new GeneratePairs<>());

        DataSet<Tuple3<K, K, Float>> scores = vertexPairs
                .groupBy(0, 1)
                .reduce(new ReduceFunction<Tuple3<K, K, Float>>() {
                    @Override
                    public Tuple3<K, K, Float> reduce(Tuple3<K, K, Float> one, Tuple3<K, K, Float> two) throws Exception {
                        return new Tuple3<>(one.f0, one.f1, one.f2 + two.f2);
                    }
                });

        return scores;
    }

    private class GenerateGroupSpans<K> implements
            GroupReduceFunction<Tuple3<K, K, Float>, Tuple4<Integer, K, K, Float>>{
        @Override
        public void reduce(Iterable<Tuple3<K, K, Float>> in, Collector<Tuple4<Integer, K, K, Float>> out){
            int groupSpan = 1;
            int count = 0;
            for(Tuple3<K, K, Float> edge : in){
                if(count++ == groupSize){
                    ++groupSpan;
                    count = 0;
                }
                out.collect(new Tuple4<>(groupSpan, edge.f0, edge.f1, edge.f2));
            }
        }
    }

    private class GenerateGroups<K> implements
            FlatMapFunction<Tuple4<Integer, K, K, Float>, Tuple4<Integer, K, K, Float>>{
        @Override
        public void flatMap(Tuple4<Integer, K, K, Float> in, Collector<Tuple4<Integer, K, K, Float>> out){
            int currSpan = in.f0;
            for(int count = 1; count <= currSpan; ++count){
                out.collect(new Tuple4<>(count, in.f1, in.f2, in.f3));
            }
        }
    }

    private class GeneratePairs<K> implements
            GroupReduceFunction<Tuple4<Integer, K, K, Float>, Tuple3<K, K, Float>> {
        @Override
        public void reduce(Iterable<Tuple4<Integer, K, K, Float>> in, Collector<Tuple3<K, K, Float>> out){
            List<Tuple4<Integer, K, K, Float>> visited = new ArrayList<>();
            Tuple3<K, K, Float> prior;
            int count = 0;

            for(Tuple4<Integer, K, K, Float> edge : in){
                for(int ix = 0; ix != count; ++ix){
                    prior = new Tuple3<>(visited.get(ix).f2, edge.f2, edge.f3);
                    out.collect(prior);
                }
                if(count < groupSize)  {
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
