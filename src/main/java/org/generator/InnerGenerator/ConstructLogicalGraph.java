package org.generator.InnerGenerator;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import static org.generator.factories.LoadVertex.returnPathToPropertiesValues;
import static org.generator.factories.VerticesFactorySingle.createVertices_MultiLevels_markRoot;

public class ConstructLogicalGraph {

    public static LogicalGraph constructLogicalGraph(GradoopFlinkConfig config, ExecutionEnvironment env, int numOfLevels, int verticesInLevel, int numOfInnerGraphs) throws Exception {

        DataSet<Tuple2<Integer, EPGMVertex>> vertices2 = createVertices_MultiLevels_markRoot(numOfLevels, returnPathToPropertiesValues(), verticesInLevel, numOfInnerGraphs, env) ;
        DataSet<EPGMVertex> vertices = vertices2.map(tuple -> tuple.f1);
        DataSet<EPGMEdge> edges = InnerEdges.createEdges_MultiInnerGraphs_sameLabel2(vertices2, env);

        return config.getLogicalGraphFactory().fromDataSets(vertices, edges);


    }
}
