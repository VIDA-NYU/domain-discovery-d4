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
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.codec.binary.Hex;
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
    
    private static final Logger LOGGER = Logger
            .getLogger(FlexibleColumnReader.class.getName());
    
    private final String _delimiter;
    private final File _file;
    private BufferedReader _in;
    private ValueCounter _value = null;
    private final int _hashLengthThreshold;
    
    /**
     * Initialize the input file and column delimiter for the reader.
     * 
     * @param file
     * @param columnId
     * @param delimiter
     * @param hashLengthThreshold
     */
    public FlexibleColumnReader(
            File file,
            int columnId,
            String delimiter,
            int hashLengthThreshold
    ) {
        
        super(columnId);
	
        _file = file;
        _delimiter = delimiter;
        _hashLengthThreshold = hashLengthThreshold;
        
        this.reset();
    }
    
    /**
     * Initialize reader with default column delimiter.
     * 
     * @param file
     * @param columnId
     * @param hashLengthThreshold
     */
    public FlexibleColumnReader(File file, int columnId, int hashLengthThreshold) {
        
        this(file, columnId, DEFAULT_DELIMITER, hashLengthThreshold);
    }

    public FlexibleColumnReader(File file, int hashLengthThreshold) {
        
        this(file, ColumnHelper.getColumnId(file), DEFAULT_DELIMITER, hashLengthThreshold);
    }

    public FlexibleColumnReader(File file) {
        
        this(file, ColumnHelper.getColumnId(file), DEFAULT_DELIMITER, -1);
    }

    @Override
    public ColumnReader<ValueCounter> cloneReader() {

        return new FlexibleColumnReader(
                _file,
                this.columnId(),
                _delimiter,
                _hashLengthThreshold
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
    
    private String hashValue(String value) {
    
        if ((_hashLengthThreshold == -1) || (value.length() < _hashLengthThreshold)) {
            return value;
        } else {
            MessageDigest digest;
            try {
                digest = MessageDigest.getInstance("SHA-256");
                String hash = Hex.encodeHexString(digest.digest(value.getBytes("UTF-8")));
                System.out.println(hash + "\t" + value);
                return hash;
            } catch (java.security.NoSuchAlgorithmException | java.io.UnsupportedEncodingException ex) {
               LOGGER.log(Level.SEVERE, null, ex);
               throw new RuntimeException(ex);
            }
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
                        _value = new ValueCounterImpl(this.hashValue(tokens[0]), 1);
                        break;
                    case 2:
                        _value = new ValueCounterImpl(
                                this.hashValue(tokens[0]),
                                Integer.parseInt(tokens[1])
                        );
                        break;
                    case 3:
                        _value = new IdentifiableValueCounterImpl(
                           Integer.parseInt(tokens[0]),
                           this.hashValue(tokens[1]),
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
