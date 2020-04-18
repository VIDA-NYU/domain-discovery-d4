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
import org.opendata.core.object.filter.AnyObjectFilter;
import org.opendata.core.object.filter.ObjectFilter;
import org.opendata.core.set.HashObjectSet;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.core.set.ImmutableIDSet;
import org.opendata.core.similarity.Support;

/**
 * Read a set of strong domains from file.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class StrongDomainReader {
   
    private final File _file;
    
    public StrongDomainReader(File file) {
        
        _file = file;
    }
    
    /**
     * Read complete set of domains. Each domain will contain the identifier of
     * equivalence classes as its elements.
     * 
     * @return
     * @throws java.io.IOException 
     */
    public IdentifiableObjectSet<StrongDomain> read() throws java.io.IOException {
        
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
    public IdentifiableObjectSet<StrongDomain> read(ObjectFilter<Integer> filter) throws java.io.IOException {
        
        HashObjectSet<StrongDomain> result = new HashObjectSet<>();
        
        try (BufferedReader in = FileSystem.openReader(_file)) {
            String line;
            while ((line = in.readLine()) != null) {
                String[] tokens = line.split("\t");
                int domainId = Integer.parseInt(tokens[0]);
                if (filter.contains(domainId)) {
                    IDSet localDomains = new ImmutableIDSet(tokens[1]);
                    IDSet columns = new ImmutableIDSet(tokens[3]);
                    HashObjectSet<StrongDomainMember> members;
                    members = new HashObjectSet<>();
                    for (String token : tokens[2].split(",")) {
                        String[] node = token.split(":");
                        int nodeId = Integer.parseInt(node[0]);
                        int colCount = Integer.parseInt(node[2]);
                        StrongDomainMember member;
                        Support weight;
                        weight = new Support(colCount, columns.length());
                        member = new StrongDomainMember(nodeId, weight.value());
                        members.add(member);
                    }
                    result.add(
                            new StrongDomain(
                                    domainId,
                                    members,
                                    localDomains,
                                    columns
                            )
                    );
                }
            }
        }
        
        return result;
    }
}
