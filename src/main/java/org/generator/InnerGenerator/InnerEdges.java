package org.generator.InnerGenerator;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
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

public class InnerEdges {

    public static DataSet<EPGMEdge> createEdges_MultiInnerGraphs_sameLabel2(DataSet<Tuple2<Integer, EPGMVertex>> vertices, ExecutionEnvironment env) {
        // Map vertices to level
        DataSet<Tuple2<Integer, EPGMVertex>> verticesWithLevel = vertices
                .map(vertex -> new Tuple2<>(vertex.f0, vertex.f1)) // level,Vertex
                .returns(new TypeHint<Tuple2<Integer, EPGMVertex>>() {}); // Flink need to know what is the type of the returned data as he could fail because of type erasure in java.

        // Join vertices with next level
        DataSet<Tuple2<EPGMVertex, EPGMVertex>> joinedVertices = verticesWithLevel
                .join(verticesWithLevel)
                .where(0)
                .equalTo(vertex -> vertex.f0 - 1)
                .with((vertex1, vertex2) -> new Tuple2<>(vertex1.f1, vertex2.f1))
                .returns(new TypeHint<Tuple2<EPGMVertex, EPGMVertex>>() {});

        // Create edges from joined vertex pairs
        return joinedVertices.flatMap(new FlatMapFunction<Tuple2<EPGMVertex, EPGMVertex>, EPGMEdge>() {
            @Override
            public void flatMap(Tuple2<EPGMVertex, EPGMVertex> vertexPair, Collector<EPGMEdge> out) {
                Random random = new Random();
                EPGMEdge edge = new EPGMEdge();
                edge.setId(GradoopId.get());
                edge.setLabel("knows");
                edge.setSourceId(vertexPair.f0.getId());
                edge.setTargetId(vertexPair.f1.getId());

                Properties properties = new Properties();
                properties.set("period", random.nextInt(60) + 1); // for how long are two persons knowing each other.
                edge.setProperties(properties);
                out.collect(edge);
            }
        }).returns(new TypeHint<EPGMEdge>() {});
    }
    }



