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

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import org.opendata.core.constraint.Threshold;
import org.opendata.core.value.ValueCounter;
import org.opendata.core.profiling.datatype.DefaultDataTypeAnnotator;
import org.opendata.core.value.DefaultValueTransformer;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.io.FileSystem;
import org.opendata.core.metric.Support;
import org.opendata.core.set.IDSet;
import org.opendata.core.util.MemUsagePrinter;
import org.opendata.db.column.ColumnReader;
import org.opendata.db.column.ValueColumnsReaderFactory;

/**
 * Create a term index file. The output file is tab-delimited and contains three
 * columns: (1) the term identifier, (2) the term, and a comma-separated list of
 * column identifier:count pairs.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TermIndexGenerator {

    private class IOTerm {

        private final IDSet _columns;
        private final String _term;

        public IOTerm(String term, IDSet columns) {
            
            _term = term;
            _columns = columns;
        }

        public IDSet columns() {

            return _columns;
        }
        
        public IOTerm merge(IOTerm t) {
            
            return new IOTerm(_term, _columns.union(t.columns()));
        }

        public String term() {

            return _term;
        }

        public void write(PrintWriter out) {

            out.println(
                    this.term() + "\t" +
                    _columns.toIntString()
            );
        }
    }

    private class TermFileMerger {
    
        public int merge(TermSetIterator reader1, TermSetIterator reader2, OutputStream os) throws java.io.IOException {

            int lineCount = 0;
            
            try (PrintWriter out = new PrintWriter(os)) {
                while ((!reader1.done()) && (!reader2.done())) {
                    IOTerm t1 = reader1.term();
                    IOTerm t2 = reader2.term();
                    int comp = t1.term().compareTo(t2.term());
                    if (comp < 0) {
                        t1.write(out);
                        reader1.next();
                    } else if (comp > 0) {
                        t2.write(out);
                        reader2.next();
                    } else {
                        t1.merge(t2).write(out);
                        reader1.next();
                        reader2.next();
                    }
                    lineCount++;
                }
                while (!reader1.done()) {
                    reader1.term().write(out);
                    reader1.next();
                    lineCount++;
                }
                while (!reader2.done()) {
                    reader2.term().write(out);
                    reader2.next();
                    lineCount++;
                }
            }
            return lineCount;
        }
    }

    private interface TermSetIterator {
    
        public boolean done();
        public void next() throws java.io.IOException;
        public IOTerm term();
    }
    
    private class TermFileReader implements TermSetIterator {
    
        private BufferedReader _in = null;
        private IOTerm _term = null;
        
        public TermFileReader(InputStream is) throws java.io.IOException {

            _in = new BufferedReader(new InputStreamReader(is));

            this.readNext();
        }

        public TermFileReader(File file) throws java.io.IOException {

            this(FileSystem.openFile(file));
        }

        @Override
        public boolean done() {

            return (_term == null);
        }

        @Override
        public void next() throws java.io.IOException {

            if ((_in != null) && (_term != null)) {
                this.readNext();
            }
        }

        private void readNext() throws java.io.IOException {

            String line = _in.readLine();
            if (line != null) {
                String[] tokens = line.split("\t");
                _term = new IOTerm(
                        tokens[0],
                        new HashIDSet(tokens[1].split((",")))
                );
            } else {
                _term = null;
                _in.close();
                _in = null;
            }
        }

        @Override
        public IOTerm term() {

            return _term;
        }
    }
    
    private class TermSetReader implements TermSetIterator {

        private final ArrayList<String> _terms;
        private final HashMap<String, HashIDSet> _termIndex;
        private int _readIndex;
        
        public TermSetReader(
                ArrayList<String> terms,
                HashMap<String, HashIDSet> termIndex
        ) {
            _terms = terms;
            _termIndex = termIndex;
            
            _readIndex = 0;
        }
        
        @Override
        public boolean done() {

            return (_readIndex >= _terms.size());
        }

        @Override
        public void next() {
            
            _readIndex++;
        }

        @Override
        public IOTerm term() {

            String term = _terms.get(_readIndex);
            return new IOTerm(term, _termIndex.get(term));
        }
    }

    public void createIndex(
            ValueColumnsReaderFactory readers,
            Threshold textThreshold,
            int bufferSize,
            File outputFile
    ) throws java.io.IOException {
        
        DefaultDataTypeAnnotator annotator = new DefaultDataTypeAnnotator();
        DefaultValueTransformer transformer = new DefaultValueTransformer();
        
        HashMap<String, HashIDSet> termIndex = new HashMap<>();
        int columnCount = 0;
        while (readers.hasNext()) {
            ColumnReader reader = readers.next();
            columnCount++;
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
            BigDecimal textFrac;
            textFrac = new Support(textCount, columnValues.size()).value();
            if (!textThreshold.isSatisfied(textFrac)) {
                continue;
            }
            for (String term : columnValues) {
                if (!termIndex.containsKey(term)) {
                    termIndex.put(term, new HashIDSet(reader.columnId()));
                } else {
                    termIndex.get(term).add(reader.columnId());
                }
                if (termIndex.size() > bufferSize) {
                    System.out.println("WRITE AT COLUMN " + columnCount);
                    writeTermIndex(termIndex, outputFile);
                    termIndex = new HashMap<>();
                }
            }
        }
        if (!termIndex.isEmpty()) {
            writeTermIndex(termIndex, outputFile);
        }
        // Add unique term id and term data type information to terms in
        // current output file.
        File tmpFile = File.createTempFile("tmp", outputFile.getName());
        try (
                BufferedReader in = FileSystem.openReader(outputFile);
                PrintWriter out = FileSystem.openPrintWriter(tmpFile)
        ) {
            String line;
            int termId = 0;
            while ((line = in.readLine()) != null) {
                String[] tokens = line.split("\t");
                out.println(termId + "\t" + tokens[0] + "\t" + tokens[1]);
                termId++;
            }
        }
        FileSystem.copy(tmpFile, outputFile);
        Files.delete(tmpFile.toPath());
    }
    
    public void run(
            List<File> files,
            Threshold textThreshold,
            int bufferSize,
            File outputFile
    ) throws java.io.IOException {
        
        // Create the directory for the output file if it does not exist.
        FileSystem.createParentFolder(outputFile);
        if (outputFile.exists()) {
            outputFile.delete();
        }
        
        this.createIndex(
                new ValueColumnsReaderFactory(files),
                textThreshold,
                bufferSize,
                outputFile
        );
    }

    private void writeTermIndex(
            HashMap<String, HashIDSet> termIndex,
            File outputFile
    ) throws java.io.IOException {

        ArrayList<String> terms = new ArrayList<>(termIndex.keySet());
        Collections.sort(terms);
	
        if (!outputFile.exists()) {
            try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
                for (String term : terms) {
                    HashIDSet columns = termIndex.get(term);
                    out.println(
                            term + "\t" +
                            columns.toIntString()
                    );
                }
            }
            System.out.println("INITIAL FILE HAS " + termIndex.size() + " ROWS.");
        } else {
            System.out.println("MERGE " + termIndex.size() + " TERMS.");
            File tmpFile = File.createTempFile("tmp", outputFile.getName());
            int count = new TermFileMerger().merge(new TermFileReader(outputFile),
                    new TermSetReader(terms, termIndex),
                    FileSystem.openOutputFile(tmpFile)
            );
            Files.copy(
                    tmpFile.toPath(),
                    outputFile.toPath(),
                    new CopyOption[]{StandardCopyOption.REPLACE_EXISTING}
            );
            Files.delete(tmpFile.toPath());
            System.out.println("MERGED FILE HAS " + count + " ROWS.");
        }
        
        new MemUsagePrinter().print("MEMORY USAGE");
    }
}
