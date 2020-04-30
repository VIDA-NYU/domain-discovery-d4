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
import org.opendata.core.set.EntitySet;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.core.util.StringHelper;
import org.opendata.curation.d4.domain.Domain;
import org.opendata.curation.d4.domain.DomainReader;
import org.opendata.curation.d4.domain.StrongDomain;
import org.opendata.curation.d4.domain.StrongDomainReader;
import org.opendata.db.Database;
import org.opendata.db.column.Column;
import org.opendata.db.eq.EQIndex;

/**
 * Print set of terms in a column.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnDomainsPrinter {
    
    private void print(IDSet nodes, EQIndex eqIndex, EntitySet columnTerms, EntitySet expansionTerms) {
        
        List<String> terms = new ArrayList<>();
        for (int nodeId : nodes) {
            for (int termId : eqIndex.get(nodeId).terms()) {
                if (columnTerms.contains(termId)) {
                    terms.add(columnTerms.get(termId).name());
                } else {
                    terms.add(expansionTerms.get(termId).name() + " (*)");
                }
            }
        }
        Collections.sort(terms);
        for (String term : terms) {
            System.out.println(term);
        }
    }
    
    /**
     * Get sorted list of terms in the column.
     * 
     * @param eqIndex
     * @param reader
     * @param localDomains
     * @param strongDomains
     * @param columnId
     * @throws java.io.IOException 
     */
    public void print(
            EQIndex eqIndex,
            EntitySetReader reader,
            IdentifiableObjectSet<Domain> localDomains,
            IdentifiableObjectSet<StrongDomain> strongDomains,
            int columnId
    ) throws java.io.IOException {
        
        Database db = new Database(eqIndex);
        Column column = db.columns().get(columnId);
        
        HashIDSet termFilter = new HashIDSet();
        for (int nodeId : column) {
            termFilter.add(eqIndex.get(nodeId).terms());
        }
        EntitySet columnTerms = reader.readEntities(termFilter);
        
        List<Domain> columnDomains = new ArrayList<>();
        for (StrongDomain strongDomain : strongDomains) {
            if (strongDomain.columns().contains(columnId)) {
                for (int locDomId : strongDomain.localDomains()) {
                    Domain localDomain = localDomains.get(locDomId);
                    if (localDomain.columns().contains(columnId)) {
                        columnDomains.add(localDomain);
                        break;
                    }
                }
            }
        }
        
        HashIDSet expansionFilter = new HashIDSet();
        for (Domain domain : columnDomains) {
            for (int nodeId : domain) {
                for (int termId : eqIndex.get(nodeId).terms()) {
                    if (!termFilter.contains(termId)) {
                        expansionFilter.add(termId);
                    }
                }
            }
        }
        EntitySet expansionTerms;
        if (!expansionFilter.isEmpty()) {
            expansionTerms = reader.readEntities(expansionFilter);
        } else {
            expansionTerms = new EntitySet();
        }
        
        HashIDSet writtenNodes = new HashIDSet();
        for (int iDomain = 0; iDomain < columnDomains.size(); iDomain++) {
            if (iDomain > 0) {
                System.out.println();
            }
            Domain domain = columnDomains.get(iDomain);
            String headline = "DOMAIN " + domain.id();
            System.out.println(headline);
            System.out.println(StringHelper.repeat("-", headline.length()));
            this.print(domain, eqIndex, columnTerms, expansionTerms);
            writtenNodes.add(domain);
        }
        
        IDSet missingNodes = column.difference(writtenNodes);
        if (!missingNodes.isEmpty()) {
            System.out.println("\nREMAINING TERMS\n--------------");
            this.print(missingNodes, eqIndex, columnTerms, expansionTerms);
        }
    }
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <eq-file>\n" +
            "  <term-file>\n" +
            "  <local-domains-file>\n" +
            "  <strong-domains-file>\n" +
            "  <column-id>";
    
    private static final Logger LOGGER = Logger
            .getLogger(ColumnDomainsPrinter.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 5) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        File termFile = new File(args[1]);
        File localDomainFile = new File(args[2]);
        File strongDomainFile = new File(args[3]);
        int columnId = Integer.parseInt(args[4]);
        
        try {
            new ColumnDomainsPrinter()
                    .print(
                            new EQIndex(eqFile),
                            new EntitySetReader(termFile),
                            new DomainReader(localDomainFile).read(),
                            new StrongDomainReader(strongDomainFile).read(),
                            columnId
                    );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
