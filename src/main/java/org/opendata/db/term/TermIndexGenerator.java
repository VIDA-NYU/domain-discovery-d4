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
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.FileListReader;
import org.opendata.core.value.ValueCounter;
import org.opendata.core.value.DefaultValueTransformer;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.io.FileSystem;
import org.opendata.core.set.IDSet;
import org.opendata.core.util.MemUsagePrinter;
import org.opendata.db.column.ColumnReader;
import org.opendata.db.column.ValueColumnsReaderFactory;

/**
 * Create a term index file. The output file is tab-delimited and contains three
 * columns: (1) the term identifier, (2) the term, and a comma-separated list of
 * column identifier.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TermIndexGenerator implements TermConsumer {
    
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

    private final int _bufferSize;
    private HashMap<String, HashIDSet> _termIndex;
    private final File _outputFile;
    private File _tmpFile;

    public TermIndexGenerator(File outputFile, int bufferSize) throws java.io.IOException {
    	
    	_outputFile = outputFile;
    	_bufferSize = bufferSize;
    	
    	_tmpFile = File.createTempFile("tmp", ".tix");

    	FileSystem.createParentFolder(_outputFile);
        if (_outputFile.exists()) {
        	_outputFile.delete();
        }
    }
    
    @Override
    public void close() {
    	
        if (!_termIndex.isEmpty()) {
            this.writeBuffer();
        }
        // Add unique term id and term data type information to terms in
        // current output file.
        try {
	        try (
	                BufferedReader in = FileSystem.openReader(_tmpFile);
	                PrintWriter out = FileSystem.openPrintWriter(_outputFile)
	        ) {
	            String line;
	            int termId = 0;
	            while ((line = in.readLine()) != null) {
	                String[] tokens = line.split("\t");
	                out.println(termId + "\t" + tokens[0] + "\t" + tokens[1]);
	                termId++;
	            }
	        }
	        Files.delete(_tmpFile.toPath());
        } catch (java.io.IOException ex) {
        	throw new RuntimeException(ex);
        }
    }

	@Override
	public void consume(Term term) {

		if (!_termIndex.containsKey(term.name())) {
            _termIndex.put(term.name(), term.columns());
        } else {
            _termIndex.get(term.name()).add(term.columns());
        }
        if (_termIndex.size() > _bufferSize) {
            this.writeBuffer();
            _termIndex = new HashMap<>();
        }
	}
   
    public void createIndex(ValueColumnsReaderFactory readers) throws java.io.IOException {
        
        DefaultValueTransformer transformer = new DefaultValueTransformer();
        
        this.open();
        
        while (readers.hasNext()) {
            ColumnReader<ValueCounter> reader = readers.next();
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
            for (String term : columnValues) {
            	this.consume(new Term(term, reader.columnId()));
            }
        }
        
        this.close();
    }
    
    public void run(List<File> files, int hashLengthThreshold) throws java.io.IOException {
        
        this.createIndex(new ValueColumnsReaderFactory(files, hashLengthThreshold));
    }

        
    public void run(List<File> files) throws java.io.IOException {
    	
        this.run(files, -1);
    }
    
    @Override
    public void open() {
    	
    	_termIndex = new HashMap<>();
    }
    
    private void writeBuffer() {

        ArrayList<String> terms = new ArrayList<>(_termIndex.keySet());
        Collections.sort(terms);
	
        try {
	        if (!_tmpFile.exists()) {
	            try (PrintWriter out = FileSystem.openPrintWriter(_tmpFile)) {
	                for (String term : terms) {
	                    HashIDSet columns = _termIndex.get(term);
	                    out.println(
	                            term + "\t" +
	                            columns.toIntString()
	                    );
	                }
	            }
	            System.out.println("INITIAL FILE HAS " + _termIndex.size() + " ROWS.");
	        } else {
	            System.out.println("MERGE " + _termIndex.size() + " TERMS.");
	            File tmpFile = File.createTempFile("tmp", ".tix");
	            int count = new TermFileMerger().merge(new TermFileReader(_tmpFile),
	                    new TermSetReader(terms, _termIndex),
	                    FileSystem.openOutputFile(tmpFile)
	            );
	            _tmpFile.delete();
	            _tmpFile = tmpFile;
	            System.out.println("MERGED FILE HAS " + count + " ROWS.");
	        }
        } catch (java.io.IOException ex) {
        	throw new RuntimeException(ex);
        }
        
        new MemUsagePrinter().print("MEMORY USAGE");
    }
    
    private final static String COMMAND =
	    "Usage:\n" +
	    "  <column-file-or-dir>\n" +
	    "  <mem-buffer-size>\n" +
            "  <hash-length-threshold>\n" +
	    "  <output-file>";
    
    public static void main(String[] args) {
        
        System.out.println("Term Index Generator (Version 0.2.2)");

        if (args.length != 4) {
            System.out.println(COMMAND);
            System.exit(-1);
        }

        File inputDirectory = new File(args[0]);
        int bufferSize = Integer.parseInt(args[1]);
        int hashLengthThreshold = Integer.parseInt(args[2]);
        File outputFile = new File(args[3]);
        
        try {
            new TermIndexGenerator(outputFile, bufferSize).run(
                    new FileListReader(".txt").listFiles(inputDirectory),
                    hashLengthThreshold
            );
        } catch (java.io.IOException ex) {
            Logger.getGlobal().log(Level.SEVERE, "CREATE TERM INDEX", ex);
            System.exit(-1);
        }
    }
}
