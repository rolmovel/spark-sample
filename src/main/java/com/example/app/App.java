package com.example.app;

import com.example.app.chain.TaskProcessor;
import com.example.app.chain.Tasks;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.max;


/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) {
      // CREATE SPARK CONTEXT
      SparkSession session =
          SparkSession
              .builder()
              .appName("bank")
              .master("local[3]")
              .getOrCreate();

      // LOAD DATASETS
      Dataset<Row> dataframe = session
          .read()
          .option("header", "true")
          .option("inferSchema", true)
          .csv("src/main/resource/bank.csv");


      new TaskProcessor()
          .andThen(Tasks::writeToDatabase)
          .andThen(Tasks::getLoanByAge)
          .andThen(Tasks::getBalanceByAgeAndMarital)
          .andThen(Tasks::getContactByAge)
          .andThen(Tasks::getBalanceByMaritalAndEducation)
          .andThen(Tasks::getJobBetweenMarriedWithHouseAndBalance)
          .apply(dataframe);

    }

}
