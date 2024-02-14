package org.generator;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class GCExecutor {


    public static void main(String[] args) throws Exception {

//        Configuration config2 = new Configuration();
//        config2.setString("taskmanager.memory.process.size", "20000m");
//        config2.setString("taskmanager.network.memory.fraction", "0.6");
//        config2.setString(AkkaOptions.ASK_TIMEOUT, "50 s");
//
//        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(config2);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Integer.parseInt(args[0]));
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

        int numOfLevels = Integer.parseInt(args[1]); // overall graph levels
        int verticesInLevel = Integer.parseInt(args[2]); //i use unified number of Vertices per level
        int numOfGraphs = Integer.parseInt(args[3]); // for graph Collections this is the number of logical graphs in a graph collection.
        int numOfInnerGraphs = Integer.parseInt(args[4]); // this parameter will define how many Inner graphs inside a logical Graph( all these graph together are a part of a logical graph structure, not a graph collection)
        String outputPath = args[5]; // this is the output path of the created Graph Collection.


        GraphCollection myCollection = ConstructCollection.constructCollection(config, env, numOfLevels, verticesInLevel, numOfGraphs, numOfInnerGraphs);

        // to be able to differentiate between graphs we name them based on # of levels,graphs and verticesInLevel.
        DataSink mySink = new DOTDataSink(outputPath + "GC_" + numOfGraphs + "-G_" + "#OfLevels-levels_" + verticesInLevel + "-VInLevel", true);
//      DataSink mySink = new CSVDataSink(outputPath + "GC_" + numOfGraphs + "-G_" + "#OfLevels-levels_" + verticesInLevel + "-VInLevel", config);

        mySink.write(myCollection);

        env.execute("");
    }
}