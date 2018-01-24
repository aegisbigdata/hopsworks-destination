/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hopsworksDestination.stage.destination.sample;

import de.fokus.fraunhofer.hopsworks.adapter.HopsworksAdapter;
import hopsworksDestination.stage.lib.sample.Errors;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.delimited.DelimitedCharDataGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Iterator;
import java.util.List;

/**
 * This target is an example and does not actually write to any destination.
 */
public abstract class SampleTarget extends BaseTarget {

  /**
   * Gives access to the UI configuration of the stage provided by the {@link SampleDTarget} class.
   */
  public abstract String getConfig();

  private static final Logger LOG = LoggerFactory.getLogger(SampleTarget.class);

  /** {@inheritDoc} */
  @Override
  protected List<ConfigIssue> init() {
    // Validate configuration values and open any required resources.
    List<ConfigIssue> issues = super.init();

    if (getConfig().equals("invalidValue")) {
      issues.add(
          getContext().createConfigIssue(
              Groups.SAMPLE.name(), "config", Errors.SAMPLE_00, "Here's what's wrong..."
          )
      );
    }

    // If issues is not empty, the UI will inform the user of each configuration issue in the list.
    return issues;
  }

  /** {@inheritDoc} */
  @Override
  public void destroy() {
    // Clean up any open resources.
    super.destroy();
  }

  /** {@inheritDoc} */
  @Override
  public void write(Batch batch) throws StageException {


    LOG.info("Writing a batch of records: " + batch.toString());

    StringWriter writer = new StringWriter();
    DataGenerator gen;
    try {
      gen = new DelimitedCharDataGenerator(writer, CsvMode.CSV.getFormat(), CsvHeader.NO_HEADER, "header", "value", false);
    } catch (IOException ioe) {
      throw new StageException(Errors.WAVE_01, ioe);
    }

    Iterator<Record> batchIterator = batch.getRecords();

    while (batchIterator.hasNext()) {
      Record record = batchIterator.next();
      try {
        gen.write(record);
      } catch (Exception e) {

      }
    }

    try {
      gen.close();
    } catch (IOException ioe) {
      throw new StageException(Errors.SAMPLE_01, ioe);
    }
    String csvBuffer = writer.toString();

    LOG.info("Buffered " + csvBuffer + " bytes of CSV data");
  }



  /**
   * Writes a single record to the destination.
   *
   * @param record the record to write to the destination.
   * @throws OnRecordErrorException when a record cannot be written.
   */
  /*private void write(Record record, StringWriter writer) throws OnRecordErrorException {
    // This is a contrived example, normally you may be performing an operation that could throw
    // an exception or produce an error condition. In that case you can throw an OnRecordErrorException
    // to send this record to the error pipeline with some details.
    if (!record.has("/someField")) {
      throw new OnRecordErrorException(Errors.SAMPLE_01, record, "exception detail message.");
    }

    writer.println(record.toString());





    // TODO: write the records to your final destination
    String filePath = "sample-files/obama.jpg";

    String projectId = "1027";
    String folder = "upload/fokus_mpo_data";

    HopsworksAdapter hopsworksAdapter = new HopsworksAdapter();
    hopsworksAdapter.actionUploadFile(projectId,folder,filePath);

  }
  */

}
