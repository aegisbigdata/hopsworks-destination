/**
 * Copyright 2015 StreamSets Inc.
 * <p>
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hopsworksDestination.stage.destination.sample;

import com.streamsets.pipeline.api.*;

import de.fokus.fraunhofer.hopsworks.adapter.HopsworksAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.util.Iterator;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;


@StageDef(
        version = 1,
        label = "AEGIS Destination",
        description = "Writes data to the AEGIS Platform",
        icon = "aegis.gif",
        recordsByRef = true,
        onlineHelpRefUrl = ""
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public class HopsworksDTarget extends HopsworksTarget {

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "default",
            label = "AEGIS Project Folder",
            displayPosition = 20,
            group = "AEGIS"
    )

    public String config;


    @ConfigDef(
            required = true,
            type = ConfigDef.Type.NUMBER,
            defaultValue = "1",
            label = "AEGIS Project ID",
            displayPosition = 10,
            group = "AEGIS"
    )

    public int projectId;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "default",
            label = "AEGIS File Name",
            displayPosition = 30,
            group = "AEGIS"
    )

    public String fileName;


    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "admin@kth.se",
            label = "AEGIS Username",
            displayPosition = 40,
            group = "AEGIS"
    )

    public String userName;


    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "admin",
            label = "AEGIS Password",
            displayPosition = 50,
            group = "AEGIS"
    )

    public String password;


    /** {@inheritDoc} */
    @Override
    public String getConfig() {
        return config;
    }

    private static final Logger LOG = LoggerFactory.getLogger(HopsworksTarget.class);
    private int fileNumber;

    public HopsworksDTarget() {
        this.fileNumber = 0;
    }


    private String nextFileName() {

        String fileName = this.fileName + "-" + this.fileNumber + ".csv";
        this.fileNumber++;
        return fileName;
    }


    /** {@inheritDoc} */
    @Override
    public void write(Batch batch) throws StageException {


        LOG.info("Writing a batch of records: " + batch.toString());

        String tmpFileName = this.nextFileName();

        PrintWriter writer = null;
        try {

            writer = new PrintWriter(new File(tmpFileName));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }


        Iterator<Record> batchIterator = batch.getRecords();

        System.out.println("Start Batch Iterator");
        while (batchIterator.hasNext()) {
            Record record = batchIterator.next();
            try {

                Iterator<String> fieldsPaths = record.getEscapedFieldPaths().iterator();
                System.out.println("Iterate over fields");
                writeRecord(record, fieldsPaths, writer);


            } catch (Exception e) {
                e.printStackTrace();

            }
        }
        writer.close();
        writeFileToHopsWorks(tmpFileName);
        removeTmpFile(tmpFileName);

    }

    private void writeRecord(Record record, Iterator<String> fieldsPaths, PrintWriter writer) {
        while (fieldsPaths.hasNext()) {
            String fieldName = fieldsPaths.next();

            if (record.has(fieldName) && fieldName.equals("") != true) {

                Field recordField = record.get(fieldName);
                String value = recordField.getValueAsString();
                System.out.println("Value:" + value);
                writer.print(value);
                if (fieldsPaths.hasNext()) {
                    writer.print(",");
                } else {
                    writer.print("\n");
                }

            }
        }

    }

    private void removeTmpFile(String filePath) {
        Path path = Paths.get(filePath);
        try {
            Files.delete(path);
        } catch (NoSuchFileException x) {
            System.err.format("%s: no such" + " file or directory%n", path);
        } catch (DirectoryNotEmptyException x) {
            System.err.format("%s not empty%n", path);
        } catch (IOException x) {
            // File permission problems are caught here.
            System.err.println(x);
        }

    }

    private void writeFileToHopsWorks(String filePath) {

        String projectId = this.projectId + "";
        String folder = "upload/" + this.config;
        HopsworksAdapter hopsworksAdapter = new HopsworksAdapter();
        hopsworksAdapter.actionUploadFile(projectId, folder, filePath);

    }

}
