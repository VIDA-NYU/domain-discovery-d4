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
package org.opendata.curation.d4.explore;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.EntitySetReader;
import org.opendata.core.object.Entity;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.core.sort.NamedObjectComparator;
import org.opendata.curation.d4.domain.Domain;
import org.opendata.curation.d4.domain.DomainReader;
import org.opendata.db.eq.EQ;
import org.opendata.db.eq.EQIndex;

/**
 * Print local domains for a column.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnLocalDomainPrinter {
   
    public void run(
            EQIndex eqIndex,
            EntitySetReader termReader,
            IdentifiableObjectSet<Domain> localDomains,
            int columnId
    ) throws java.io.IOException {
        
        HashIDSet columnTerms = new HashIDSet();
        for (int nodeId : eqIndex.columns().get(columnId)) {
            columnTerms.add(eqIndex.get(nodeId).terms());
        }

        List<HashIDSet> columnDomains = new ArrayList<>();
        HashIDSet domainTerms = new HashIDSet();
        for (Domain domain : localDomains) {
            if (domain.columns().contains(columnId)) {
                HashIDSet dom = new HashIDSet();
                for (int nodeId : domain) {
                    EQ node = eqIndex.get(nodeId);
                    dom.add(node.terms());
                    domainTerms.add(node.terms());
                }
                columnDomains.add(dom);
            }
        }
        
        List<Entity> terms = termReader.readEntities(domainTerms).toList();
        Collections.sort(terms, new NamedObjectComparator());
        
        for (Entity term : terms) {
            String line = term.name();
            if (!columnTerms.contains(term.id())) {
                line += " (*)";
            }
            for (HashIDSet domain : columnDomains) {
                if (domain.contains(term.id())) {
                    line += "\tX";
                } else {
                    line += "\t-";
                }
            }
            System.out.println(line);
        }
    }
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <eq-file>\n" +
            "  <term-file>\n" +
            "  <local-domains-file>\n" +
            "  <column-id>";
    
    private static final Logger LOGGER = Logger
            .getLogger(ColumnLocalDomainPrinter.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 4) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        File termFile = new File(args[1]);
        File localDomainFile = new File(args[2]);
        int columnId = Integer.parseInt(args[3]);
        
        try {
            new ColumnLocalDomainPrinter().run(
                    new EQIndex(eqFile),
                    new EntitySetReader(termFile),
                    new DomainReader(localDomainFile).read(),
                    columnId
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
