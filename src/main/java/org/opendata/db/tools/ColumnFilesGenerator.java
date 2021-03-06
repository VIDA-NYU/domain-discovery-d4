/*
 * Copyright 2020 New York University.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opendata.db.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.FileSystem;
import org.opendata.curation.d4.Constants;

/**
 * Extract dataset columns into column files. Takes a list of columns that are
 * exported and writes all column files into a single output directory.
 * 
 * The format of the input file is tab-delimited with three columns:
 * 
 * 1) domain
 * 2) dataset identifier
 * 3) column names
 * 
 * Assumes that generated all column files are for datasets that were downloaded
 * on the same day, i.e., that all dataset files are in folders domain/date/tsv.
 * 
 * The generated files have names that follow the pattern:
 * 
 * columns-name.column-index.dataset-id.txt
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnFilesGenerator {
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <base-directory>\n" +
            "  <input-file>\n" +
            "  <file-type> ['columns', 'datasets']\n" +
            "  <download-date>\n" +
            "  <cache-size>\n" +
            "  <output-dir>";
    
    private static final Logger LOGGER = Logger
            .getLogger(ColumnFilesGenerator.class.getName());
    
    public static void main(String[] args) {
    
        System.out.println(Constants.NAME + " - Column Files Generator - Version " + Constants.VERSION);
        
        if (args.length != 6) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File baseDir = new File(args[0]);
        File inputFile = new File(args[1]);
        String fileType = args[1].toLowerCase();
        String downloadDate = args[3];
        int cacheSize = Integer.parseInt(args[4]);
        File outputDir = new File(args[5]);

        List<File> files = new ArrayList<>();
        ColumnFactory factory = null;
        
        if (fileType.equals("columns")) {
            HashMap<String, HashSet<String>> columns = new HashMap<>();
            try (BufferedReader in = FileSystem.openReader(inputFile)) {
                String line;
                while ((line = in.readLine()) != null) {
                    String[] tokens = line.split("\t");
                    String datasetId = tokens[1];
                    String columnName = tokens[2];
                    if (!columns.containsKey(datasetId)) {
                        columns.put(datasetId, new HashSet<>());
                        String filename = String.format(
                                "%s%s%s%stsv%s%s.tsv.gz",
                                tokens[0],
                                File.separator,
                                downloadDate,
                                File.separator,
                                File.separator,
                                datasetId
                        );
                        File file = FileSystem.joinPath(baseDir, filename);
                        if (file.exists()) {
                            files.add(file);
                        } else {
                            System.out.println(String.format("File %s does not exists.", filename));
                        }
                    }
                    columns.get(datasetId).add(columnName);
                }
                factory = new ColumnIndexFactory(columns, cacheSize, outputDir);
                System.out.println(String.format("Process %d datasets", files.size()));
                new Dataset2ColumnsConverter(factory, true).run(files, 1);
            } catch (java.lang.InterruptedException | java.io.IOException ex) {
                LOGGER.log(Level.SEVERE, "READ", ex);
                System.exit(-1);
            }
        } else {
            try (BufferedReader in = FileSystem.openReader(inputFile)) {
                String line;
                while ((line = in.readLine()) != null) {
                    String[] tokens = line.split("\t");
                    String datasetId = tokens[1];
                    String filename = String.format(
                            "%s%s%s%stsv%s%s.tsv.gz",
                            tokens[0],
                            File.separator,
                            downloadDate,
                            File.separator,
                            File.separator,
                            datasetId
                    );
                    File file = FileSystem.joinPath(baseDir, filename);
                    if (file.exists()) {
                        files.add(file);
                    } else {
                        System.out.println(String.format("File %s does not exists.", filename));
                    }
                }
            } catch (java.io.IOException ex) {
                LOGGER.log(Level.SEVERE, "READ", ex);
                System.exit(-1);
            }
            // Create output directory if it does not exist
            FileSystem.createFolder(outputDir);
            File columnFile = FileSystem.joinPath(outputDir, "columns.tsv");
            try (PrintWriter out = FileSystem.openPrintWriter(columnFile)) {
                factory = new DatasetColumnFactory(outputDir, cacheSize, out);
                System.out.println(String.format("Process %d datasets", files.size()));
                new Dataset2ColumnsConverter(factory, true).run(files, 1);
            } catch (java.lang.InterruptedException | java.io.IOException ex) {
                LOGGER.log(Level.SEVERE, "RUN", ex);
                System.exit(-1);
            }
        }
    }
}
