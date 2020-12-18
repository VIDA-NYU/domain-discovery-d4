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
package org.opendata.db.term;

import java.io.File;
import java.math.BigDecimal;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.constraint.Threshold;
import org.opendata.core.io.FileListReader;
import org.opendata.core.value.ValueCounter;
import org.opendata.core.profiling.datatype.DefaultDataTypeAnnotator;
import org.opendata.core.value.DefaultValueTransformer;
import org.opendata.core.io.FileSystem;
import org.opendata.core.metric.Support;
import org.opendata.core.util.FormatedBigDecimal;
import org.opendata.curation.d4.Constants;
import org.opendata.db.column.ColumnReader;
import org.opendata.db.column.FlexibleColumnReader;

/**
 * Create a term index file. The output file is tab-delimited and contains three
 * columns: (1) the term identifier, (2) the term, and (3) a comma-separated
 * list of column identifier.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TermIndexGenerator {

    private class TermGeneratorTask implements Runnable {

        private final ConcurrentLinkedQueue<File> _queue;
        private final TermIndexFile _termIndex;
        private final Threshold _textThreshold;
        private final boolean _verbose;
        
        public TermGeneratorTask(
                ConcurrentLinkedQueue<File> queue,
                Threshold textThreshold,
                boolean verbose,
                TermIndexFile termIndex
        ) {
            _queue = queue;
            _textThreshold = textThreshold;
            _verbose = verbose;
            _termIndex = termIndex;
        }
        
        @Override
        public void run() {
            
            DefaultDataTypeAnnotator annotator = new DefaultDataTypeAnnotator();
            DefaultValueTransformer transformer = new DefaultValueTransformer();

            File file = null;
            while ((file = _queue.poll()) != null) {
                ColumnReader reader = new FlexibleColumnReader(file);
                Date start = new Date();
                HashSet<String> columnValues = new HashSet<>();
                while (reader.hasNext()) {
                    ValueCounter colVal = reader.next();
                    if (!colVal.isEmpty()) {
                        String term = transformer.transform(colVal.getText());
                        if (!columnValues.contains(term)) {
                            columnValues.add(term);
                        }
                    }
                }
                if (columnValues.isEmpty()) {
                    continue;
                }
                int textCount = 0;
                for (String term : columnValues) {
                    if (annotator.getType(term).isText()) {
                        textCount++;
                    }
                }
                Date end = new Date();
                BigDecimal textFrac;
                textFrac = new Support(textCount, columnValues.size()).value();
                if (_verbose) {
                    System.out.println(
                            String.format(
                                    "%s (%d ms) [%s]",
                                    file.getName(),
                                    (end.getTime() - start.getTime()),
                                    new FormatedBigDecimal(textFrac).toString()
                            )
                    );
                }
                if (!_textThreshold.isSatisfied(textFrac)) {
                    continue;
                }
                final int columnId = reader.columnId();
                for (String term : columnValues) {
                    _termIndex.add(term, columnId);
                }
            }
        }        
    }
    
    public void run(
            List<File> files,
            Threshold textThreshold,
            int bufferSize,
            boolean verbose,
            int threads,
            File outputFile
    ) throws java.lang.InterruptedException, java.io.IOException {
        
        // Create the directory for the output file if it does not exist.
        FileSystem.createParentFolder(outputFile);
        if (outputFile.exists()) {
            outputFile.delete();
        }
        
        ConcurrentLinkedQueue<File> queue;
        queue = new ConcurrentLinkedQueue<>(files);
        
        TermIndexFile termIndex;
        termIndex = new TermIndexFile(bufferSize, verbose);
        
        ExecutorService es = Executors.newCachedThreadPool();
        for (int iThread = 0; iThread < threads; iThread++) {
            es.execute(
                    new TermGeneratorTask(
                            queue,
                            textThreshold,
                            verbose,
                            termIndex
                    )
            );
        }
        es.shutdown();
        es.awaitTermination(threads, TimeUnit.DAYS);
        
        termIndex.write(outputFile);
    }
    
    private final static String COMMAND =
	    "Usage:\n" +
	    "  <column-file-or-dir>\n" +
            "  <text-threshold>\n" +
	    "  <mem-buffer-size>\n" +
            "  <threads>\n" +
	    "  <output-file>";
    
    public static void main(String[] args) {
        
	System.out.println(Constants.NAME + " - Term Index Generator - Version (" + Constants.VERSION + ")\n");

        if (args.length != 5) {
            System.out.println(COMMAND);
            System.exit(-1);
        }

        File inputDirectory = new File(args[0]);
        Threshold textThreshold = Threshold.getConstraint(args[1]);
        int bufferSize = Integer.parseInt(args[2]);
        int threads = Integer.parseInt(args[3]);
        File outputFile = new File(args[4]);
        
        try {
            new TermIndexGenerator().run(
                    new FileListReader(".txt").listFiles(inputDirectory),
                    textThreshold,
                    bufferSize,
                    true,
                    threads,
                    outputFile
            );
        } catch (java.lang.InterruptedException | java.io.IOException ex) {
            Logger.getGlobal().log(Level.SEVERE, "CREATE TERM INDEX", ex);
            System.exit(-1);
        }
    }
}
