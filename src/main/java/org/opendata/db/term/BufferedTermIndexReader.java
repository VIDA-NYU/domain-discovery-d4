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
import org.opendata.core.io.FileSystem;
import org.opendata.core.set.HashIDSet;

/**
 * Read a term index file one by one.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class BufferedTermIndexReader implements AutoCloseable, Comparable<BufferedTermIndexReader> {
    
    private final File _file;
    private BufferedReader _in = null;
    private Term _term = null;
    
    public BufferedTermIndexReader(File file) throws java.io.IOException {
	
        _file = file;
        
        _in = FileSystem.openReader(_file);
        this.next();
    }

	@Override
	public void close() {

		if (_in != null) {
			try {
				_in.close();
				_in = null;
			} catch (java.io.IOException ex) {
				throw new RuntimeException(ex);
			}
		}
	}

	@Override
	public int compareTo(BufferedTermIndexReader reader) {

		return _term.name().compareTo(reader.peek().name());
	}
   
	public String directory() {
	
		return _file.getParent();
	}
	
	public boolean hasNext() {
		
		return (_term != null);
	}
	
    public Term next() throws java.io.IOException {
        
    	Term result = _term;
    	
    	if (_in != null) {
	        String line = _in.readLine();
	        if (line != null) {
                String[] tokens = line.split("\t");
                int termId = Integer.parseInt(tokens[0]);
                HashIDSet columns = new HashIDSet(tokens[2].split(","));
                _term = new Term(termId, tokens[1], columns);
	        } else {
	        	this.close();
	        	_term = null;
	        }
    	}
        return result;
    }
    
    public Term peek() {
    	
    	return _term;
    }
}
