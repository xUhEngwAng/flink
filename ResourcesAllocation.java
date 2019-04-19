package com.xun.flink.graph;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.undirected.VertexDegree;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class ResourcesAllocation<K, VV, EV> {
  public DataSet<Tuple3<K, K, Float>> run(Graph<K, VV, EV> input) throws Exception{
        DataSet<Tuple2<K, Float>> inverseDegree = input
                .run(new VertexDegree<K, VV, EV>())
                .map(new MapFunction<Vertex<K, LongValue>, Tuple2<K, Float>>() {
                    @Override
                    public Tuple2<K, Float> map(Vertex<K, LongValue> in){
                        return new Tuple2<K, Float>(in.f0, 1f/in.f1.getValue());
                    }
                });

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
                });

        DataSet<Tuple3<K, K, Float>> vertexPairs = srcInverseDegree
                .groupBy(0)
                .sortGroup(1, Order.ASCENDING)
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

    public static class GeneratePairs<K> implements
            GroupReduceFunction<Tuple3<K, K, Float>, Tuple3<K, K, Float>> {
        @Override
        public void reduce(Iterable<Tuple3<K, K, Float>> in, Collector<Tuple3<K, K, Float>> out){
            List<Tuple3<K, K, Float>> visited = new ArrayList<>();
            Tuple3<K, K, Float> prior;
            int count = 0;
            for(Tuple3<K, K, Float> edge : in){
                for(int ix = 0; ix != count; ++ix){
                    prior = new Tuple3<>(visited.get(ix).f1, edge.f1, edge.f2);
                    out.collect(prior);
                }
                ++count;
                visited.add(edge);
            }
        }
    }
}
