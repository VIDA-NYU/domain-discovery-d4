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

import org.opendata.core.value.ValueCounter;

/**
 * Read a stream of column values. The reader needs to be reset before ever
 * iterations through the data (except for the first one).
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 * @param <T>
 */
public abstract class ColumnReader<T extends ValueCounter> implements AutoCloseable {
    
    private final int _columnId;
    
    public ColumnReader(int columnId) {
	
	_columnId = columnId;
    }
    
    /**
     * Create a copy of the reader. Primarily necessary if multiple threads
     * want to read a stream in parallel.
     * 
     * @return 
     */
    public abstract ColumnReader<T> cloneReader();
    /**
     * Close the reader.
     * 
     */
    public abstract void close();
    
    /**
     * The unique identifier of the column that is being read.
     * 
     * @return 
     */
    public int columnId() {
	
	return _columnId;
    }
    
    /**
     * Check if the reader has a next value or if the last value has been
     * reached.
     * 
     * @return 
     */
    public abstract boolean hasNext();

    /**
     * Get next value in the stream. Result is null if end of stream has been
     * reached.
     * 
     * @return 
     */
    
    public abstract T next();
    /**
     * Reset the reader to the beginning of the stream.
     * 
     */
    public abstract void reset();
}
