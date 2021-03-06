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

import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.opendata.core.io.FileSystem;

/**
 * Convert a set of Socrata dataset files in column files that contain the set
 * of distinct terms for each column.
 * 
 * Converts all files in the given input directory that have suffix .tsv or
 * .tsv.gz. Generates a tab-delimited columns file containing unique column
 * identifier, the column name (which is the last element in the unique path),
 * and the dataset identifier.
 * 
 * Uses multiple threads, each converting different sets of input files.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class Dataset2ColumnsConverter {
    
    private class DatasetConverter implements Runnable {

        private final ColumnFactory _columnFactory;
        private final ConcurrentLinkedQueue<File> _files;
        private final boolean _verbose;
        
        public DatasetConverter(
                ConcurrentLinkedQueue<File> files,
                ColumnFactory columnFactory,
                boolean verbose
        ) {
            _files = files;
            _columnFactory = columnFactory;
            _verbose = verbose;
        }
        
        private CSVParser tsvParser(File file) throws java.io.IOException {

            return new CSVParser(
                    new InputStreamReader(FileSystem.openFile(file)),
                    CSVFormat.TDF
                            .withFirstRecordAsHeader()
                            .withIgnoreHeaderCase()
                            .withIgnoreSurroundingSpaces(false)
            );
        }

        @Override
        public void run() {
    
            File file;
            while ((file = _files.poll()) != null) {
                String dataset;
                if (file.getName().endsWith(".tsv")) {
                    dataset = file.getName().substring(0, file.getName().length() - 4);
                } else if (file.getName().endsWith(".tsv.gz")) {
                    dataset = file.getName().substring(0, file.getName().length() - 7);
                } else {
                    return;
                }
                if (_verbose) {
                    System.out.println(file.getName());
                }
                try (CSVParser in = this.tsvParser(file)) {
                    List<ColumnHandler> columns = new ArrayList<>();
                    int colIndex = 0;
                    for (String colName : in.getHeaderNames()) {
                        columns.add(_columnFactory.getHandler(dataset, colIndex++, colName));
                    }
                    for (CSVRecord row : in) {
                        for (int iColumn = 0; iColumn < row.size(); iColumn++) {
                            String term = row.get(iColumn);
                            if (!term.equals("")) {
                                columns.get(iColumn).add(term);
                            }
                        }
                    }
                    for (ColumnHandler column : columns) {
                        column.close();
                    }
                } catch (java.io.IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
            
            if (_verbose) {
                System.out.println("DONE");
            }
        }        
    }
    
    private final ColumnFactory _columnFactory;
    private final boolean _verbose;
    
    public Dataset2ColumnsConverter(
            File outputDir,
            PrintWriter out,
            int cacheSize,
            boolean verbose
    ) {
        _columnFactory = new DefaultColumnFactory(outputDir, cacheSize, out);
        _verbose = verbose;
    }
    
    public Dataset2ColumnsConverter(
            ColumnFactory columnFactory,
            boolean verbose
    ) {
        _columnFactory = columnFactory;
        _verbose = verbose;
    }
    
    /**
     * Convert a list of dataset files into a set of column files.
     * 
     * @param files
     * @param threads
     * @throws java.lang.InterruptedException
     * @throws java.io.IOException 
     */
    public void run(List<File> files, int threads) throws java.lang.InterruptedException, java.io.IOException {

        if (_verbose) {
            System.out.println(String.format("CONVERT %d DATASETS", files.size()));
        }

        ConcurrentLinkedQueue<File> queue = new ConcurrentLinkedQueue<>(files);
        
        if (threads > 1) {
            ExecutorService es = Executors.newCachedThreadPool();
            for (int iThread = 0; iThread < threads; iThread++) {
                es.execute(new DatasetConverter(queue, _columnFactory, _verbose));
            }
            es.shutdown();
            es.awaitTermination(threads, TimeUnit.DAYS);        
        } else {
            new DatasetConverter(queue, _columnFactory, _verbose).run();
        }
    }
}
