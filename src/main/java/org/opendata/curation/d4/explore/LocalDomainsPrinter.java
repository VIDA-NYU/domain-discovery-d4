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
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.EntitySetReader;
import org.opendata.core.io.FileSystem;
import org.opendata.core.set.EntitySet;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.core.util.StringHelper;
import org.opendata.curation.d4.domain.Domain;
import org.opendata.curation.d4.domain.DomainReader;
import org.opendata.db.eq.EQIndex;

/**
 * Print terms for all local domains.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class LocalDomainsPrinter {
    
    private void print(
    		IDSet nodes,
    		EQIndex eqIndex,
    		EntitySet columnTerms,
    		PrintWriter out
    ) {
        List<String> terms = new ArrayList<>();
        for (int nodeId : nodes) {
            for (int termId : eqIndex.get(nodeId).terms()) {
                if (columnTerms.contains(termId)) {
                    terms.add(columnTerms.get(termId).name());
                }
            }
        }
        Collections.sort(terms);
        for (String term : terms) {
            System.out.println(term);
            out.println(term);
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
            PrintWriter out
    ) throws java.io.IOException {
        
        HashIDSet termFilter = new HashIDSet();
        for (Domain domain : localDomains) {
	        for (int nodeId : domain) {
	            termFilter.add(eqIndex.get(nodeId).terms());
	        }
        }
        EntitySet columnTerms = reader.readEntities(termFilter);
        
        List<Domain> domains = localDomains.toList();
        Collections.sort(domains, new Comparator<Domain>(){

			@Override
			public int compare(Domain dom0, Domain dom1) {
				return Integer.compare(
						dom1.columns().length(),
						dom0.columns().length()
				);
			}});
        
        boolean isFirst = true;
        for (Domain domain : domains) {
        	if (!isFirst) {
        		out.println("\n\n");
        		System.out.println("\n\n");
        	} else {
                isFirst = false;
        	}
            String headline = "DOMAIN " + domain.id() + ", " + domain.columns().length() + " COLUMN(S)";
            out.println(headline);
            out.println(StringHelper.repeat("-", headline.length()));
            System.out.println(headline);
            System.out.println(StringHelper.repeat("-", headline.length()));
            this.print(domain, eqIndex, columnTerms, out);
        }
    }
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <eq-file>\n" +
            "  <term-file>\n" +
            "  <local-domains-file>\n" +
            "  <output-file>";
    
    private static final Logger LOGGER = Logger
            .getLogger(LocalDomainsPrinter.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 4) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        File termFile = new File(args[1]);
        File localDomainFile = new File(args[2]);
        File outputFile = new File(args[3]);
        
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            new LocalDomainsPrinter()
                    .print(
                            new EQIndex(eqFile),
                            new EntitySetReader(termFile),
                            new DomainReader(localDomainFile).read(),
                            out
                    );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
