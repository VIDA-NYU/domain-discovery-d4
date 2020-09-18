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
import org.opendata.core.value.IdentifiableValueCounterImpl;
import org.opendata.core.value.ValueCounter;
import org.opendata.core.value.ValueCounterImpl;
import org.opendata.core.io.FileSystem;

/**
 * Reads a file containing the content of a column with unique value identifiers
 * and values counts. Default delimiter is tab.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class FlexibleColumnReader extends ColumnReader<ValueCounter> {
   
    public static final String DEFAULT_DELIMITER = "\t";
    
    private final String _delimiter;
    private final File _file;
    private BufferedReader _in;
    private ValueCounter _value = null;
    
    /**
     * Initialize the input file and column delimiter for the reader.
     * 
     * @param file
     * @param columnId
     * @param delimiter
     */
    public FlexibleColumnReader(
            File file,
            int columnId,
            String delimiter
    ) {
        
        super(columnId);
	
        _file = file;
        _delimiter = delimiter;
        
        this.reset();
    }
    
    /**
     * Initialize reader with default column delimiter.
     * 
     * @param file
     * @param columnId
     */
    public FlexibleColumnReader(File file, int columnId) {
        
        this(file, columnId, DEFAULT_DELIMITER);
    }

    public FlexibleColumnReader(File file) {
        
        this(file, ColumnHelper.getColumnId(file), DEFAULT_DELIMITER);
    }

    @Override
    public ColumnReader<ValueCounter> cloneReader() {

        return new FlexibleColumnReader(
                _file,
                this.columnId(),
                _delimiter
        );
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
                String[] tokens = line.split(_delimiter);
                switch (tokens.length) {
                    case 1:
                        _value = new ValueCounterImpl(tokens[0], 1);
                        break;
                    case 2:
                        _value = new ValueCounterImpl(
                                tokens[0],
                                Integer.parseInt(tokens[1])
                        );
                        break;
                    case 3:
                        _value = new IdentifiableValueCounterImpl(
                           Integer.parseInt(tokens[0]),
                           tokens[1],
                           Integer.parseInt(tokens[2])
                        );
                       break;
                    default:
                        throw new java.lang.RuntimeException("Unexpected token count in file " + _file.getAbsolutePath() + " (Line " + line + "):" + tokens.length);
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
