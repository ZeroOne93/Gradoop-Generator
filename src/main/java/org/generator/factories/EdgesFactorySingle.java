package org.generator.factories;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.Properties;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class EdgesFactorySingle {
    // another method for creating edges between the vertices of the same inner graph but without considering the unique identifiers within the label of vertices within same inner graph.
    public static DataSet<EPGMEdge> createEdges_MultiInnerGraphs_sameLabel(DataSet<Tuple2<Integer, EPGMVertex>> vertices, ExecutionEnvironment env) {
        return vertices.coGroup(vertices)
                .where(0).equalTo(vertex -> vertex.f0 - 1)
                .with(new CoGroupFunction<Tuple2<Integer, EPGMVertex>, Tuple2<Integer, EPGMVertex>, EPGMEdge>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<Integer, EPGMVertex>> currentLevel,
                                        Iterable<Tuple2<Integer, EPGMVertex>> nextLevel,
                                        Collector<EPGMEdge> out) {

                        List<Tuple2<Integer, EPGMVertex>> nextLevelVertices = new ArrayList<>();
                        nextLevel.forEach(nextLevelVertices::add);

                        Random random = new Random(); // Initialize Random here

                        for (Tuple2<Integer, EPGMVertex> vertex1 : currentLevel) {
                            for (Tuple2<Integer, EPGMVertex> vertex2 : nextLevelVertices) {
                                // Check if vertices belong to the same inner graph based on their graphId property. this condition could be commented when creating a graph collection.
//                                if (vertex1.f1.getProperties().get("graphId").equals(vertex2.f1.getProperties().get("graphId"))) {
                                EPGMEdge edge = new EPGMEdge();
                                edge.setId(GradoopId.get());
                                edge.setLabel("knows");
                                edge.setSourceId(vertex1.f1.getId());
                                edge.setTargetId(vertex2.f1.getId());

                                // Add property "period" with a random value between 1 and 30
                                Properties properties = new Properties();
                                properties.set("period", random.nextInt(30) + 1);
                                edge.setProperties(properties);
                                out.collect(edge);
//                                }
                            }
                        }
                    }
                })
                .returns(TypeInformation.of(new TypeHint<EPGMEdge>() {
                }));
    }
}
