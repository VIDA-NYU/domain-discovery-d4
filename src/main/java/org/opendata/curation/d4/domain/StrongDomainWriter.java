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

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import org.opendata.core.io.FileSystem;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IdentifiableIDSet;
import org.opendata.core.set.IdentifiableObjectSet;

/**
 * Write a stream of strong domains to a text file. Outputs a tab-delimited
 * line for each strong domain with the following components:
 * 
 * - domain id
 * - comma-separated list of local domains in the support set
 * - comma-separated list of pairs (separated by ':') of equivalence class id
 *   and number of domains in the support set that the equivalence class occurs
 *   in
 * - comma-separated list of column ids for all columns the domain occurs in
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class StrongDomainWriter implements StrongDomainConsumer {

    private final File _file;
    private final IdentifiableObjectSet<Domain> _localDomains;
    private PrintWriter _out;
    
    public StrongDomainWriter(
            File file,
            IdentifiableObjectSet<Domain> localDomains
    ) {
    
        _file = file;
        _localDomains = localDomains;
        
        _out = null;
    }
    
    @Override
    public void close() {

        if (_out != null) {
            _out.close();
            _out = null;
        }
    }

    @Override
    public void consume(IdentifiableIDSet domain) {

        List<Domain> domainSet = new ArrayList<>();
        HashIDSet domainColumns = new HashIDSet();
        HashIDSet domainNodes = new HashIDSet();
        for (int domId : domain) {
            Domain locDomain = _localDomains.get(domId);
            domainSet.add(locDomain);
            domainColumns.add(locDomain.columns());
            domainNodes.add(locDomain);
        }

        _out.print(domain.id() +"\t" + domain.toIntString());
        String delim = "\t";
        for (int nodeId : domainNodes) {
            int domCount = 0;
            HashIDSet nodeColumns = new HashIDSet();
            for (Domain locDomain : domainSet) {
                if (locDomain.contains(nodeId)) {
                    domCount++;
                    nodeColumns.add(locDomain.columns());
                }
            }
            int colCount = nodeColumns.length();
            _out.print(delim + nodeId + ":" + domCount + ":" + colCount);
            delim = ",";
        }
        _out.println("\t" + domainColumns.toIntString());
    }

    @Override
    public void open() {

        try {
            _out = FileSystem.openPrintWriter(_file);
        } catch (java.io.IOException ex) {
            throw new RuntimeException(ex);
        }
    }    
}
