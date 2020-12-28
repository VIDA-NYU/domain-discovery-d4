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
package org.opendata.curation.d4.domain;

import java.io.BufferedReader;
import java.io.File;
import org.opendata.core.io.FileSystem;
import org.opendata.core.object.AnyObjectFilter;
import org.opendata.core.object.ObjectFilter;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.HashObjectSet;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.core.set.ImmutableIDSet;

/**
 * Read a given database domain from file.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DomainReader implements DomainStream {
   
    private final File _file;
    
    public DomainReader(File file) {
        
        _file = file;
    }
    
    /**
     * Read complete set of domains. Each domain will contain the identifier of
     * equivalence classes as its elements.
     * 
     * @return
     * @throws java.io.IOException 
     */
    public IdentifiableObjectSet<Domain> read() throws java.io.IOException {
        
        return this.read(new AnyObjectFilter());
    }
    
    /**
     * Read complete set of domains. Each domain will contain the identifier of
     * equivalence classes as its elements. Include only those domains in the
     * result set whose identifier are contained in the given filter.
     * 
     * @param filter
     * @return
     * @throws java.io.IOException 
     */
    public IdentifiableObjectSet<Domain> read(ObjectFilter<Integer> filter) throws java.io.IOException {
        
        HashObjectSet<Domain> result = new HashObjectSet<>();
        
        try (BufferedReader in = FileSystem.openReader(_file)) {
            String line;
            while ((line = in.readLine()) != null) {
                String[] tokens = line.split("\t");
                int domainId = Integer.parseInt(tokens[0]);
                if (filter.contains(domainId)) {
                    IDSet nodes = new ImmutableIDSet(tokens[1]);
                    IDSet columns;
                    if (tokens.length >= 3) {
                        columns = new ImmutableIDSet(tokens[2]);
                    } else {
                        columns = new HashIDSet();
                    }
                    result.add(new Domain(domainId, nodes, columns));
                }
            }
        }
        
        return result;
    }
    
    @Override
    public void stream(DomainConsumer consumer) throws java.io.IOException {

        consumer.open();
        
        try (BufferedReader in = FileSystem.openReader(_file)) {
            String line;
            while ((line = in.readLine()) != null) {
                String[] tokens = line.split("\t");
                int domainId = Integer.parseInt(tokens[0]);
                IDSet nodes = new ImmutableIDSet(tokens[1]);
                IDSet columns;
                if (tokens.length >= 3) {
                    columns = new ImmutableIDSet(tokens[2]);
                } else {
                    columns = new HashIDSet();
                }
                consumer.consume(new Domain(domainId, nodes, columns));
            }
        }
        
        consumer.close();
    }
}
