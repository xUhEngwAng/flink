package com.xun.flink.graph;

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

import java.util.ArrayList;
import java.util.List;

public class CommonNeighbors<K, VV, EV> {
  public DataSet<Tuple3<K, K, Integer>> run(Graph<K, VV, EV> input){
        DataSet<Tuple2<K, K>> vertexPairs = input
                .getEdges()
                .groupBy(0)
                .sortGroup(1, Order.ASCENDING)
                .reduceGroup(new GeneratePairs<K, EV>());

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

    public static class GeneratePairs<K, EV> implements
            GroupReduceFunction<Edge<K, EV>, Tuple2<K, K>> {
        @Override
        public void reduce(Iterable<Edge<K,EV>> in, Collector<Tuple2<K, K>> out){
            List<Edge<K, EV>> visited = new ArrayList<>();
            Tuple2<K, K> prior;
            int count = 0;
            for(Edge<K, EV> edge : in){
                for(int ix = 0; ix != count; ++ix){
                    prior = new Tuple2<>(visited.get(ix).f1, edge.f1);
                    out.collect(prior);
                }
                ++count;
                visited.add(edge);
            }
        }
    }
}
