package org.generator.factories;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.Properties;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Map;
public class VerticesFactorySingle {
    private static long calculateTotalVertices(int numOfLevels, int verticesInLevel, int numOfGraphs) {
        long total = 0;
        for (int level = 0; level < numOfLevels; level++) {
            total += (long) (level == 0 ? 1 : verticesInLevel) * numOfGraphs;
        }
        return total;
    }

    /**
     * @param numOfLevels
     * @param pathToValues
     * @param verticesInLevel
     * @param numOfGraphs
     * @param env
     * @return returns Tuple2 DataSet<Tuple2<Integer,EPGMVertex>> where the Integer is the level of this vertex. Vertices have a property called "isRoot" where only the root vertex has the value true.
     * @throws Exception
     * ff stands for firstName female
     * fm stands for firstName male
     */
    public static DataSet<Tuple2<Integer, EPGMVertex>> createVertices_MultiLevels_markRoot(
            int numOfLevels,
            String pathToValues,
            int verticesInLevel,
            int numOfGraphs,
            ExecutionEnvironment env) throws Exception {

        Map<String, ArrayList<String>> loadedProps = LoadVertex.loadPropsWithValuesOptimized(pathToValues);

        long totalVertices = calculateTotalVertices(numOfLevels, verticesInLevel, numOfGraphs);

        return env.generateSequence(1, totalVertices)
                .map(new MapFunction<Long, Tuple2<Integer, EPGMVertex>>() {
                    @Override
                    public Tuple2<Integer, EPGMVertex> map(Long value) throws Exception {
                        SecureRandom random = new SecureRandom();

                        long levelVal = (value - 1) % ((long) numOfLevels * verticesInLevel);
                        // Reverse the level assignment here
                        int level = numOfLevels - 1 - (int) (levelVal / verticesInLevel);
                        int index = (int) (levelVal % verticesInLevel);

                        Properties properties = new Properties();
                        properties.set("age", Integer.parseInt(loadedProps.get("age").get(random.nextInt(loadedProps.get("age").size()))));
                        properties.set("firstName", loadedProps.get(random.nextInt(2) + 1 == 1 ? "ff" : "fm").get(random.nextInt(loadedProps.get("ff").size())));
                        properties.set("gender", random.nextInt(2) + 1 == 1 ? "female" : "male");
                        EPGMVertex vertex = new EPGMVertex();
                        vertex.setLabel("person");
                        vertex.setProperties(properties);
                        vertex.setId(GradoopId.get());

                        boolean isRoot = (level == 0 && index == 0);
                        vertex.setProperty("isRoot", isRoot);

                        return new Tuple2<>(level, vertex);
                    }
                }).rebalance();
    }// when this method used for creating the vertices we should use rebalance, otherwise it will create the vertices using only one taskslot.

}