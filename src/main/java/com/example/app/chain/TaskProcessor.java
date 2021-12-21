package com.example.app.chain;

import org.apache.spark.sql.Dataset;

import java.util.function.Function;

public class TaskProcessor implements Function<Dataset, Dataset> {

  public TaskProcessor() {

  }

  @Override
  public Dataset apply(Dataset dataset) {
    return dataset;
  }

}
