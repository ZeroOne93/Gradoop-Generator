package org.generator;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.generator.factories.LoadVertex;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import static org.generator.factories.EdgesFactorySingle.createEdges_MultiInnerGraphs_sameLabel;
import static org.generator.factories.VerticesFactorySingle.createVertices_MultiLevels_markRoot;

public class ConstructCollection {
    public static GraphCollection constructCollection(GradoopFlinkConfig config, ExecutionEnvironment env, int numOfLevels, int verticesInLevel, int numOfGraphs, int numOfInnerGraphs) throws Exception {
        LogicalGraph[] myGraphs = new LogicalGraph[numOfGraphs];
        for (int i = 0; i < numOfGraphs; i++) {
            DataSet<Tuple2<Integer, EPGMVertex>> vertices2 = createVertices_MultiLevels_markRoot(numOfLevels, LoadVertex.returnPathToPropertiesValues(), verticesInLevel, numOfInnerGraphs, env);
            DataSet<EPGMVertex> vertices = vertices2.map(tuple -> tuple.f1);

            DataSet<EPGMEdge> edges = createEdges_MultiInnerGraphs_sameLabel(vertices2, env);
            myGraphs[i] = config.getLogicalGraphFactory().fromDataSets(vertices, edges);
        }

        return config.getGraphCollectionFactory().fromGraphs(myGraphs);
    }
}
