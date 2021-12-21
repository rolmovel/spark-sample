package com.example.app.chain;

import org.apache.spark.sql.Dataset;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;

public class Tasks {

  public static Dataset writeToDatabase(Dataset input) {

    input
        .write()
        .format("jdbc")
        .option("url", "jdbc:postgresql://127.0.0.1:5432/postgres?currentSchema=bank")
        .option("dbtable", "bank_sample")
        .option("user", "postgres")
        .option("password", "postgres")
        .mode("append")
        .save();

    return input;
  }

  public static Dataset getLoanByAge(Dataset input) {

    Dataset aggrConPrestamo = input
        .select(col("age"), col("loan"))
        .where(col("loan").equalTo("yes"))
        .groupBy("age")
        .agg(count(col("age")).as("total"))
        .sort(col("total").desc());

    aggrConPrestamo.show(50);

    return input;
  }

  public static Dataset getBalanceByAgeAndMarital(Dataset input) {

    Dataset aggrBalance = input
        .select(col("age"), col("marital"),col("balance"))
        .groupBy("age", "marital")
        .agg(avg(col("balance")).as("avg_balance"))
        .sort(col("avg_balance").desc());

    aggrBalance.show(50);

    return input;
  }

  public static Dataset getContactByAge(Dataset input) {

    Dataset aggrContacto = input
        .select(col("contact"))
        .where(col("age").geq(25).and(col("age").leq(35)))
        .groupBy(col("contact"))
        .agg(count(col("contact")).as("total_contact"))
        .sort(col("total_contact").desc());

    aggrContacto.show(50);

    return input;
  }

  public static Dataset getBalanceByMaritalAndEducation(Dataset input) {

    Dataset aggrBalanceCMEMin = input
        .select(col("campaign"),col("marital"),col("education"),col("balance"))
        .groupBy(col("campaign"),col("marital"),col("education"))
        .agg(min(col("balance")).as("min"))
        .sort(col("campaign").desc(),col("marital").desc(),col("education").desc());

    Dataset aggrBalanceCMEMax = input
        .select(col("campaign"),col("marital"),col("education"),col("balance"))
        .groupBy(col("campaign"),col("marital"),col("education"))
        .agg(max(col("balance")).as("max"))
        .sort(col("campaign").desc(),col("marital").desc(),col("education").desc());

    Dataset aggrBalanceCMEAvg = input
        .select(col("campaign"),col("marital"),col("education"),col("balance"))
        .groupBy(col("campaign"),col("marital"),col("education"))
        .agg(avg(col("balance")).as("avg"))
        .sort(col("campaign").desc(),col("marital").desc(),col("education").desc());

    aggrBalanceCMEMin.show(50);
    aggrBalanceCMEMax.show(50);
    aggrBalanceCMEAvg.show(50);

    return input;
  }

  public static Dataset getJobBetweenMarriedWithHouseAndBalance(Dataset input) {

    Dataset aggrTrabajo = input
        .select(col("job"),col("marital"),col("marital"),col("housing"),col("balance"))
        .where(col("marital").equalTo("married").and(col("housing").equalTo("yes")).and(col("balance").geq(1200)))
        .groupBy(col("job"))
        .agg(count(col("job")).as("total"))
        .sort(col("total").desc());

    aggrTrabajo.show(50);

    return input;
  }

}
