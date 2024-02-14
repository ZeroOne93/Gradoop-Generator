package org.generator.InnerGenerator;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class LGExecutor {


    public static void main(String[] args) throws Exception {
// User can define specific Configurations, then creating Flink Environment based on these configurations
//        Configuration config2 = new Configuration();
//        config2.setString("taskmanager.memory.process.size", "25000m");
//        config2.setString("taskmanager.network.memory.fraction", "0.5");
//        config2.setString(AkkaOptions.ASK_TIMEOUT, "25 s");//
//        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(config2);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(Integer.parseInt(args[0]));
        String outputPath = args[1]; // The Path where the created graph will be saved.

        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

        int numOfLevels = Integer.parseInt(args[2]); // number of levels in the logical graph
        int verticesInLevel = Integer.parseInt(args[3]); // number of vertices in one level in the logical graph, i use a unified number of vertices per level.
        int numOfInnerGraphs = Integer.parseInt(args[4]); // number of inner graphs inside a logical graph, its not a graph collection.


        LogicalGraph myGraph = ConstructLogicalGraph.constructLogicalGraph(config, env, numOfLevels, verticesInLevel, numOfInnerGraphs);
//        System.out.println(myGraph.getVertices().count());
//        System.out.println(myGraph.getEdges().count());

        DataSink mySink = new DOTDataSink(outputPath + "LG_" + numOfInnerGraphs + "-G_" + "5-levels_" + verticesInLevel + "-VInLevel", true);
        mySink.write(myGraph);

        env.execute("");
    }
}
