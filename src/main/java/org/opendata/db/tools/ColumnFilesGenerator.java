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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.FileSystem;
import org.opendata.core.value.DefaultValueTransformer;
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
    
    class LocalColumnFactory implements ColumnFactory {

        private final int _cacheSize;
        private final HashMap<String, HashSet<String>> _columns;
        private final File _outputDir;
        
        public LocalColumnFactory(
                HashMap<String, HashSet<String>> columns,
                int cacheSize,
                File outputDir
        ) {
            _columns = columns;
            _outputDir = outputDir;
            _cacheSize = cacheSize;
        }
        
        @Override
        public ColumnHandler getHandler(String dataset, int columnIndex, String columnName) {

            HashSet<String> columns = _columns.get(dataset);
            if (columns.contains(columnName)) {
                String filename = String.format("%s.%d.%s.txt.gz", columnName.replaceAll("\\s+", "_").trim().replaceAll("/", "_"), columnIndex, dataset);
                columns.remove(columnName);
                try {
                    return new ColumnHandler(
                        FileSystem.joinPath(_outputDir, filename),
                        new DefaultValueTransformer(),
                        _cacheSize
                    );
                } catch (java.io.IOException ex) {
                    LOGGER.log(Level.SEVERE, filename, ex);
                }
            }
            return new ColumnHandler();
        }
    }
    
    public void run(
            File baseDir,
            File columnsFile,
            String downloadDate,
            int cacheSize,
            int threads,
            File outputDir
    ) throws java.lang.InterruptedException, java.io.IOException {
        
        // Create a mapping of file names to the list of columns that are
        // extracted from that file.
        HashMap<String, HashSet<String>> columns = new HashMap<>();
        List<File> files = new ArrayList<>();
        try (BufferedReader in = FileSystem.openReader(columnsFile)) {
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
        }
        
        int columnCount = 0;
        for (HashSet<String> cols : columns.values()) {
            columnCount += cols.size();
        }
        System.out.println(String.format("\n%d columns from %d datasets\n", columnCount, columns.size()));
        new Dataset2ColumnsConverter(new LocalColumnFactory(columns, cacheSize, outputDir), true)
                .run(files, threads);
        
        System.out.println("\nMissing columns:");
        for (String dataset : columns.keySet()) {
            for (String column : columns.get(dataset)) {
                System.out.println(String.format("%s\t%s", dataset, column));
            }
        }
    }
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <base-directory>\n" +
            "  <columns-file>\n" +
            "  <download-date>\n" +
            "  <cache-size>\n" +
            "  <threads>\n" +
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
        File columnsFile = new File(args[1]);
        String downloadDate = args[2];
        int cacheSize = Integer.parseInt(args[3]);
        int threads = Integer.parseInt(args[4]);
        File outputDir = new File(args[5]);
        
        try {
            new ColumnFilesGenerator()
                    .run(baseDir, columnsFile, downloadDate, cacheSize, threads, outputDir);
        } catch (java.lang.InterruptedException | java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
