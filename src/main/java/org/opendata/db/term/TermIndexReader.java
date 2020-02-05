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
import org.opendata.core.object.filter.AnyObjectFilter;
import org.opendata.core.object.filter.ObjectFilter;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IdentifiableObjectSet;

/**
 * Read a term index file as a stream.Passes each term to a TermStreamHandler.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TermIndexReader {
    
    private final File _file;
    
    public TermIndexReader(File file) {
	
        _file = file;
    }
    
    public void read(TermConsumer consumer) throws java.io.IOException {
        
        consumer.open();
        
        try (BufferedReader in = FileSystem.openReader(_file)) {
	    String line;
	    while ((line = in.readLine()) != null) {
		String[] tokens = line.split("\t");
                consumer.consume(
                        new Term(
                                Integer.parseInt(tokens[0]),
                                tokens[1],
                                new HashIDSet(tokens[2].split(","))
                        )
                );
            }
        }
        
        consumer.close();
    }
    
    public IdentifiableObjectSet<Term> read(ObjectFilter<Integer> filter) throws java.io.IOException {
        
        TermBuffer consumer = new TermBuffer(filter);
        
        this.read(consumer);
        
        return consumer.terms();
    }
    
    public IdentifiableObjectSet<Term> read() throws java.io.IOException {
        
        return this.read(new AnyObjectFilter());
    }
}
