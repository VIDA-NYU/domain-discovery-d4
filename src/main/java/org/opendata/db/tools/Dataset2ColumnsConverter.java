/*
 * This file is part of the Data-Driven Domain Discovery Tool (D4).
 * 
 * Copyright (c) 2018-2020 New York University.
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

import java.io.BufferedWriter;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.opendata.core.io.FileSystem;
import org.opendata.core.util.count.Counter;

/**
 * Convert a set of Socrata dataset files in column files that contain the set
 * of distinct terms for each column.
 * 
 * Converts all files in the given input directory that have suffix .tsv or
 * .tsv.gz. Generates a tab-delimited columns file containing unique column
 * identifier, the column name (which is the last element in the unique path),
 * and the dataset identifier.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class Dataset2ColumnsConverter {
    
    private static final String ENV_THRESHOLD = "SOCRATA_FILETHRESHOLD";
    
    private static final Logger LOGGER = Logger
            .getLogger(Dataset2ColumnsConverter.class.getName());
    
    private final Counter _counter;
    private final long _fileSizeThreshold;
    private final File _outputDir;
    private final CSVPrinter _statsWriter;
    private final boolean _toUpper;
    
    public Dataset2ColumnsConverter(
            File outputDir,
            CSVPrinter out,
            boolean toUpper,
            long fileSizeThreshold
    ) {
        _outputDir = outputDir;
        _statsWriter = out;
        _toUpper = toUpper;
        _fileSizeThreshold  = fileSizeThreshold;
        
        _counter = new Counter(0);
    }

    public Dataset2ColumnsConverter(
            File outputDir,
            CSVPrinter out,
            boolean toUpper
    ) {
        this(outputDir, out, toUpper, getThresholdFromEnv());
    }
    
    private ColumnHandler getHandler(File inputFile, String columnName) throws java.io.IOException {

        int columnId = _counter.inc();
        File outputFile = FileSystem.joinPath(
                _outputDir,
                columnId + ".txt.gz"
        );
        if ((_fileSizeThreshold > 0) && (inputFile.length() > _fileSizeThreshold)) {
            return new ExternalColumnValueList(
                    outputFile,
                    columnId,
                    columnName,
                    _toUpper
            );
        } else {
            return new ValueSetIndex(
                outputFile,
                columnId,
                columnName,
                _toUpper
            );
        }
    }

    private static long getThresholdFromEnv() {
        
        String value = System.getenv(ENV_THRESHOLD);
        if (value != null) {
            try {
                return Long.parseLong(value);
            } catch (java.lang.NumberFormatException ex) {
                LOGGER.log(Level.SEVERE, ENV_THRESHOLD, ex);
            }
        }
        return -1;
    }
    
    /**
     * Convert a list of dataset files into a set of column files.
     * 
     * @param files
     * @throws java.io.IOException 
     */
    public void run(List<File> files) throws java.io.IOException {

        int count = 0;
        for (File file : files) {
            String dataset;
            if (file.getName().endsWith(".tsv")) {
                dataset = file.getName().substring(0, file.getName().length() - 4);
            } else if (file.getName().endsWith(".tsv.gz")) {
                dataset = file.getName().substring(0, file.getName().length() - 7);
            } else {
                return;
            }
            System.out.println((++count) + " of " + files.size() + ": " + file.getName());
            try (CSVParser in = SocrataHelper.tsvParser(file)) {
                List<ColumnHandler> columns = new ArrayList<>();
                for (String colName : in.getHeaderNames()) {
                    columns.add(this.getHandler(file, colName));
                }
                int rowCount = 0;
                for (CSVRecord row : in) {
                    for (int iColumn = 0; iColumn < row.size(); iColumn++) {
                        String term = row.get(iColumn);
                        if (!term.equals("")) {
                            columns.get(iColumn).add(term);
                        }
                    }
                    rowCount++;
                    if ((rowCount % 10000000) == 0) {
                        System.out.println(rowCount + " @ " + new java.util.Date());
                    }
                }
                for (ColumnHandler column : columns) {
                    ColumnStats stats = column.write();
                    _statsWriter.printRecord(
                            column.id(),
                            dataset,
                            column.name(),
                            stats.distinctCount(),
                            stats.totalCount(),
                            rowCount
                    );
                }
            }
        }
    }
    
    private final static String COMMAND =
            "Usage:\n" +
            "  <input-file>\n" +
            "  <to-upper>\n" +
            "  <output-dir>";
    
    public static void main(String[] args) {
        
        System.out.println("Convert Datasets to Columns (Version 0.3.0)");

        if (args.length != 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File inputFile = new File(args[0]);
        boolean toUpper = Boolean.parseBoolean(args[1]);
        File outputDir = new File(args[2]);
        
        List<File> files = new ArrayList<>();
        files.add(inputFile);
        
        File columnsDir = FileSystem.joinPath(outputDir, "columns");
        File columnsFile = FileSystem.joinPath(outputDir, "columns.tsv");

        try (BufferedWriter out = FileSystem.openBufferedWriter(columnsFile)) {
            CSVPrinter csvPrinter = new CSVPrinter(out, CSVFormat.TDF);
            new Dataset2ColumnsConverter(columnsDir, csvPrinter, toUpper).run(files);
            csvPrinter.flush();
        } catch (java.io.IOException ex) {
            Logger.getGlobal().log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
