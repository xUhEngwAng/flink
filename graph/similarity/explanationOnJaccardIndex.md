In this file, I'll simply explain how the `JaccardIndex.java` in flink's library works.

## Jaccard Index
For how Jaccard Index is computed, please refer to this [JaccardInedx][JaccardIndex]

## Procedure

### Overview
For every two vertices with at least one common neighbor, a Jaccard Index will be computed using the algorithm above, which is really simple if we have access to all neighbors of each vertex, then what we need is a for loop to compute the Jaccard index within each pair of vertices.

But the problem is that the above algorithm isn't quite distributable, because we need to access the a certain vertex and its neighbors at the same time. Still, flink doesn't provide us much accesses to a vertex's neighbors, and it's quite time-consuming to compute them manually.

Due to the various problems, flink adopts a rather poor-readability way to calcute Jaccard Index, which we'll discuss below.

I have argued that we need access to a vertex's neighbors to compute the Jaccard Index, while we cannot. But consider if there are other ways to obtain this neighborhood information -- edges of course! If we have all the edges(which is simple, because gelly provides us a `getEdges()` method), grouping on the source(or target) vertex of those edges, the edges inside a group share a common neighbor(the source/target vertex). Then we generate vertex pairs and compute their Jaccard Index.

As a result, the algorithm can be divided into the following part:
+ compute the degree for all vertices and assign it to the edge whose target vertex is the current vertex
+ get all the edges and group on source vertex, generate group pairs inside one group
+ the number of sum degrees is the sum of two edges, while the number of shared neighbors is the frequency of occurence of one vertex pair.

### Parallelism

+ consider when we generate pairs of neighbors, the number of pairs is (n choose 2 with n being the number of the group elements). If some groups are fairly large while some are quite small, the parallelism cannot be effective

+ So we divide each group into subgroups, with the scale of each subgroup being the same(the last one can be smaller), then we calculate pairs within each subgroup(which is implemented in `GenerateGroupSpans` and `GenerateGroups`)

+ Then how about the inter-subgroup pairs? In order to take care of this, we need to add the elements in latter subgroups into preceding subgroups, but we only handle the pairs within the first subgroup and the inter-subgroup pairs. In other words, pairs within the latter subgroups is generated afterwards.This is implemented in `GenerateGroupPairs`.
