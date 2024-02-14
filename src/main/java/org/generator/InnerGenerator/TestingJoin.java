package org.generator.InnerGenerator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class TestingJoin {


    /**
     * Writes the specified information to a CSV file, creating the directory if it doesn't exist.
     *
     * @param filePath           The path of the directory where the CSV file will be saved.
     * @param fileName           The name of the CSV file.
     * @param runtimeInSeconds   Runtime of the job in seconds.
     * @param parallelism        The parallelism level.
     */
    public static void writeToCsv(String filePath, String fileName,  double runtimeInSeconds, int parallelism) {
        File directory = new File(filePath);
        if (!directory.exists()) {
            directory.mkdirs(); // Create the directory if it doesn't exist
        }

        File file = new File(directory, fileName);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, true))) {
            // Check if the file is new and write the header
            if (file.length() == 0) {
                writer.write("RuntimeInSeconds,Parallelism\n");
            }

            // Write the data
            writer.write( runtimeInSeconds + "," + parallelism + "\n");
        } catch (IOException e) {
            System.out.println("There was an error writing the results");
        }
    }


    /**
     *
     * @param args
     * @throws Exception
     * The Goal of this class is to test the boundaries of the broadcast join in Flink
     */
    public static void main(String[] args) throws Exception {

        // we need one very large dataset and one very small dataset to test the optimized join using the broadcast-join.
        long large = Integer.parseInt(args[0]); // Number of elements in the large dataset.
        long small = Integer.parseInt(args[1]); // Number of elements in the small dataset.

        String joinType = args[2]; // Join type, it will be used to identify which join type was used.
        String resultPath =args[3]; // Path for the statistics.
        String outputPath2 = args[4]; // Path for the result dataset.

        int numOfParallelSubTasks = Integer.parseInt(args[5]); // Number of parallel subtasks

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(numOfParallelSubTasks);

        // Generate the large dataset (Integers from 1 to large )
        DataSet<Long> largeDataSet = env.generateSequence(1, large);
        // Generate the small dataset (integers from 1 to small)
        DataSet<Long> smallDataSet = env.generateSequence(1, small);

        // Map the datasets to a common format for joining (key-value pairs)
        DataSet<Tuple2<Integer, Integer>> largeDataSetPairs = largeDataSet.flatMap(new PairMapper());
        DataSet<Tuple2<Integer, Integer>> smallDataSetPairs = smallDataSet.flatMap(new PairMapper());

        // Perform the join operation
        DataSet<Tuple2<Integer, Integer>> joinedDataSet = largeDataSetPairs.join(smallDataSetPairs)
                .where(0) // key of the first dataset
                .equalTo(0) // key of the second dataset
                .with(new JoinFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> join(Tuple2<Integer, Integer> first, Tuple2<Integer, Integer> second) {
                        return new Tuple2<>(first.f0, first.f1 + second.f1);
                    }
                });

       // write the resulted dataset
       joinedDataSet.writeAsCsv(outputPath2+"Output/", FileSystem.WriteMode.OVERWRITE);

        // Execute the Flink job and get the execution result
        env.execute("Join Job");

        // write statistics
        writeToCsv(resultPath, joinType, env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS), env.getParallelism());


    }

    // Mapper to create key-value pairs of the generated two datasets.
    public static final class PairMapper implements FlatMapFunction<Long, Tuple2<Integer, Integer>> {
        @Override
        public void flatMap(Long value, Collector<Tuple2<Integer, Integer>> out) {
            out.collect(new Tuple2<>(value.intValue(), 1));
        }

    }
}
