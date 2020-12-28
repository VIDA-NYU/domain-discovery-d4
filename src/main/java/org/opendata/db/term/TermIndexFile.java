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
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import org.opendata.core.io.FileSystem;
import org.opendata.core.object.IdentifiableInteger;
import org.opendata.core.sort.IdentifiableObjectSort;
import org.opendata.core.util.OrderedLinkedList;
import org.opendata.core.util.IdentifiableCount;
import org.opendata.core.value.ValueCounter;
import org.opendata.db.column.ColumnHelper;

/**
 * Generator for a unique index of database terms. Creates the unique term index
 * in a memory buffer. If the buffer is full it will be written to disk. At the
 * end of index generation all files that were written to disk are merged (in a
 * single scan) to generate the final term index output file.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TermIndexFile {
    
    private class IOTerm implements Comparable<IOTerm> {

        private final List<IdentifiableInteger> _columns;
        private final String _term;

        public IOTerm(String term, List<IdentifiableInteger> columns) {
            
            _term = term;
            _columns = columns;
        }

        public List<IdentifiableInteger> columns() {

            return _columns;
        }

        @Override
        public int compareTo(IOTerm t) {

            return _term.compareTo(t.name());
        }
        public String name() {

            return _term;
        }

    }

    private class TermFileMerger {
    
        private final PrintWriter _out;
        private int _termId = 0;
        
        public TermFileMerger(PrintWriter out) {
            
            _out = out;
        }
        
        public int merge(List<TermSetIterator> readers) throws java.io.IOException {

            OrderedLinkedList<TermSetIterator> activeReaders;
            activeReaders = new OrderedLinkedList<>();
            for (TermSetIterator reader : readers) {
                if (reader.term() != null) {
                    activeReaders.add(reader);
                }
            }
            Collections.sort(activeReaders);
            
            while (activeReaders.size() > 1) {
                // The list of active readers is always sorted in decreasing
                // order of thier terms.
                // Start by outputting the term for the first reader (and all
                // following readers with the same term. Keep track of the
                // readers that need to be advanced.
                List<TermSetIterator> advanceReaders = new ArrayList<>();
                TermSetIterator reader = activeReaders.pop();
                advanceReaders.add(reader);
                String name = reader.term().name();
                List<IdentifiableInteger> columns = reader.term().columns();
                while (!activeReaders.isEmpty()) {
                    if (activeReaders.peek().compareTo(reader) == 0) {
                        advanceReaders.add(activeReaders.pop());
                    } else {
                        break;
                    }
                }
                if (advanceReaders.size() > 1) {
                    // Merge the column lists from all terms at the top.
                    columns = new ArrayList<>();
                    for (TermSetIterator r : advanceReaders) {
                        columns.addAll(r.term().columns());
                        if (r.next()) {
                            activeReaders.insert(r);
                        }
                    }
                } else {
                    // No need to merge anything. Write the term for the first
                    // reader.
                    if (reader.next()) {
                        activeReaders.insert(reader);
                    }
                }
                this.write(name, columns);
            }
            if (activeReaders.size() == 1) {
                // Write terms for the remaining reader.
                TermSetIterator reader = activeReaders.pop();
                while (reader.term() != null) {
                    this.write(reader.term());
                    reader.next();
                }
            }
            
            return _termId;
        }
        
        private void write(IOTerm term) {
            
            this.write(term.name(), term.columns());
        }
        
        private void write(String term, List<IdentifiableInteger> columns) {
            
            Collections.sort(columns, new IdentifiableObjectSort());

            String cols = ColumnHelper.toArrayString(columns.iterator());
            _out.println(String.format("%d\t%s\t%s", _termId++, term, cols));
        }
    }

    private abstract class TermSetIterator implements Comparable<TermSetIterator> {
    
        public abstract boolean next();
        public abstract IOTerm term();

        @Override
        public int compareTo(TermSetIterator reader) {

            return this.term().compareTo(reader.term());
        }
    }
    
    private class TermFileReader extends TermSetIterator {
    
        private BufferedReader _in = null;
        private IOTerm _term = null;
        
        public TermFileReader(File file) throws java.io.IOException {

            _in = new BufferedReader(new InputStreamReader(FileSystem.openFile(file)));

            this.readNext();
        }

        @Override
        public boolean next() {

            if (_in != null) {
                this.readNext();
            }
            
            return (_term != null);
        }

        private void readNext() {

            String line = null;
            try {
                line = _in.readLine();
            } catch (java.io.IOException ex) {
                throw new RuntimeException(ex);
            }
            
            if (line != null) {
                String[] tokens = line.split("\t");
                List<IdentifiableInteger> columns = new ArrayList<>();
                for (IdentifiableInteger el : ColumnHelper.fromArbitraryArrayString(tokens[1])) {
                    columns.add(el);
                }
                _term = new IOTerm(
                        tokens[0],
                        columns
                );
            } else {
                _term = null;
                try {
                    _in.close();
                } catch (java.io.IOException ex) {
                    throw new RuntimeException(ex);
                }
                _in = null;
            }
        }
        
        @Override
        public IOTerm term() {
            
            return _term;
        }
    }
    
    private class TermSetReader extends TermSetIterator {

        private final ArrayList<String> _terms;
        private final HashMap<String, List<IdentifiableInteger>> _termIndex;
        private int _readIndex;
        private IOTerm _term = null;
        
        public TermSetReader(HashMap<String, List<IdentifiableInteger>> termIndex) {
            
            _termIndex = termIndex;
            
            _terms = new ArrayList<>(termIndex.keySet());
            Collections.sort(_terms);
            _readIndex = 0;
            
            this.next();
        }

        @Override
        public final boolean next() {
            
            if (_readIndex < _terms.size()) {
                String term = _terms.get(_readIndex++);
                _term = new IOTerm(term, _termIndex.get(term));
            } else {
                _term = null;
            }
            return (_term != null);
        }
        
        @Override
        public IOTerm term() {
            
            return _term;
        }
    }

    private HashMap<String, List<IdentifiableInteger>> _buffer;
    private final List<File> _files;
    private final int _maxBufferSize;
    private final boolean _verbose;
    
    public TermIndexFile(int maxBufferSize, boolean verbose) {
        
        _maxBufferSize = maxBufferSize;
        
        _buffer = new HashMap<>();
        _files = new ArrayList<>();
        _verbose = verbose;
    }
    
    public synchronized void add(int columnId, ValueCounter term) {
        
        String key = term.getText();
        IdentifiableInteger colCount;
        colCount = new IdentifiableCount(columnId, term.getCount());
        if (!_buffer.containsKey(key)) {
            ArrayList<IdentifiableInteger> columns = new ArrayList<>();
            columns.add(colCount);
            _buffer.put(key, columns);
        } else {
            _buffer.get(key).add(colCount);
        }
        if (_buffer.size() > _maxBufferSize) {
            try {
                this.writeBuffer();
            } catch (java.io.IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }
    
    public void write(File outputFile, boolean validate) throws java.io.IOException {
        
        List<TermSetIterator> readers = new ArrayList<>();
        if (!_buffer.isEmpty()) {
            readers.add(new TermSetReader(_buffer));
        }

        for (File file : _files) {
            readers.add(new TermFileReader(file));
        }

        if (_verbose) {
            System.out.println(String.format("MERGE %d FILES.", readers.size()));
        }
        
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            int count = new TermFileMerger(out).merge(readers);
            if (_verbose) {
                System.out.println(String.format("RESULTING FILE HAS %d TERMS.", count));
            }
        }
        
        for (File file : _files) {
            file.delete();
        }
        
        if (validate) {
            if (_verbose) {
                System.out.println("VALIDATE @ " + new Date());
            }
            try (BufferedReader in = FileSystem.openReader(outputFile)) {
                String line = in.readLine();
                if (line == null) {
                    return;
                }
                String prevTerm = line.split("\t")[1];
                while ((line = in.readLine()) != null) {
                    String term = line.split("\t")[1];
                    if (prevTerm.compareTo(term) >= 0) {
                        throw new RuntimeException(String.format("%s before %s", prevTerm, term));
                    }
                    prevTerm = term;
                }
            }
            if (_verbose) {
                System.out.println("DONE @ " + new Date());
            }
        }
    }
    
    private void writeBuffer() throws java.io.IOException {
        
        File file = File.createTempFile("buf", ".tmp", new File("."));
        if (_verbose) {
            System.out.println();
            System.out.println("WRITE BUFFER TO " + file.getAbsolutePath());
            System.out.println("START @ " + new Date());
        }

        ArrayList<String> terms = new ArrayList<>(_buffer.keySet());
        Collections.sort(terms);

        if (_verbose) {
            System.out.println("SORTED @ " + new Date());
        }
        
        try (PrintWriter out = FileSystem.openPrintWriter(file)) {
            for (String term : terms) {
                String cols = ColumnHelper.toArrayString(_buffer.get(term).iterator());
                out.println(String.format("%s\t%s", term, cols));
            }
        }

        if (_verbose) {
            System.out.println("DONE @ " + new Date());
            System.out.println();
        }

        _buffer = new HashMap<>();
        _files.add(file);
    }
}
