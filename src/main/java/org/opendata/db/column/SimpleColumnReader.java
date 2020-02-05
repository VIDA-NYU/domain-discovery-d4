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
package org.opendata.db.column;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.value.ValueCounter;
import org.opendata.core.value.ValueCounterImpl;
import org.opendata.core.io.FileSystem;

/**
 * Reads a file containing one term per line. All lines are expected to contain
 * a unique term.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SimpleColumnReader extends ColumnReader<ValueCounter> {
   
    private final File _file;
    private BufferedReader _in;
    private ValueCounter _value = null;
    
    /**
     * Initialize the input file and column identifier.
     * 
     * @param file
     * @param columnId
     */
    public SimpleColumnReader(File file, int columnId) {
        
	super(columnId);
	
        _file = file;
        
        this.reset();
    }

    @Override
    public ColumnReader<ValueCounter> cloneReader() {

        return new SimpleColumnReader(_file, this.columnId());
    }

    @Override
    public void close() {
        
        if (_in != null) {
            try {
                _in.close();
            } catch (java.io.IOException ex) {
            }
            _in = null;
        }
    }
    
    @Override
    public boolean hasNext() {

        return (_value != null);
    }

    @Override
    public final ValueCounter next() {

        ValueCounter result = _value;
        
        if (_in != null) {
            String line = null;
            try {
                line = _in.readLine();
            } catch (java.io.IOException ex) {
                try {
                    _in.close();
                } catch (java.io.IOException ioEx) {
                }
                throw new java.lang.RuntimeException(ex);
            }
            if (line != null) {
                if (line.contains("\t")) {
                    Logger.getGlobal().log(Level.WARNING, "TAB-DELIMITED VALUE\n{0}", line);
                    this.next();
                } else {
                    _value = new ValueCounterImpl(line, 1);
                }
            } else {
                try {
                    _in.close();
                } catch (java.io.IOException ex) {
                }
                _in = null;
                _value = null;
            }
        }
        return result;
    }

    /**
     * Read the whole stream of column values. Requires to keep all value
     * counters in main memory.
     * 
     * @return
     */
    public List<ValueCounter> readAll() {
        
        ArrayList<ValueCounter> values = new ArrayList<>();
        
        while (this.hasNext()) {
            values.add(this.next());
        }
        
        return values;
    }

    @Override
    public final void reset() {

        try {
            _in = new BufferedReader(new InputStreamReader(FileSystem.openFile(_file)));
        } catch (java.io.IOException ex) {
            throw new java.lang.RuntimeException(ex);
        }
        this.next();
    }
}
