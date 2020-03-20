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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.FileSystem;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.db.eq.EQIndex;
import org.opendata.db.term.Term;
import org.opendata.db.term.TermIndexReader;

/**
 * Write terms in a strong domain to file. Outputs a tab-delimited text line
 * for each term with the following components:
 * 
 * - term
 * - size of the support set for the strong domain
 * - comma-separated list of local domains (from the support set) that the term
 *   occurs in
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ExportStrongDomains {
    
    public void run(
            File eqFile,
            File termFile,
            File localDomainFile,
            File strongDomainFile,
            File outputDir
    ) throws java.io.IOException {
        
        // Create the output directory if it does not exist
        FileSystem.createFolder(outputDir);
        
        // Read local domains
        IdentifiableObjectSet<Domain> localDomains;
        localDomains = new DomainReader(localDomainFile).read();
        
        // Read the strong domains
        IdentifiableObjectSet<StrongDomain> strongDomains;
        strongDomains = new StrongDomainReader(strongDomainFile).read();
        
        // Collect term identifier for all equivalence classes in the 
        // strong domains.
        EQIndex eqIndex = new EQIndex(eqFile);
        
        HashIDSet termFilter = new HashIDSet();
        for (StrongDomain domain : strongDomains) {
            for (StrongDomainMember node : domain.members()) {
                termFilter.add(eqIndex.get(node.id()).terms());
            }
        }
        
        // Read information about all terms
        IdentifiableObjectSet<Term> terms;
        terms = new TermIndexReader(termFile).read(termFilter);
        
        // Write each domain to a separate file in the output directory
        for (StrongDomain domain : strongDomains) {
            String filename = domain.id() + ".txt.gz";
            File outputFile = FileSystem.joinPath(outputDir, filename);
            int supportSetSize = domain.localDomains().length();
            try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
                for (StrongDomainMember node : domain.members()) {
                    HashIDSet nodeDoms = new HashIDSet();
                    for (int locDomId : domain.localDomains()) {
                        if (localDomains.get(locDomId).contains(node.id())) {
                            nodeDoms.add(locDomId);
                        }
                    }
                    for (int termId : eqIndex.get(node.id()).terms()) {
                        if (terms.contains(termId)) {
                            Term term = terms.get(termId);
                            out.println(
                                    term.name() + "\t" +
                                    supportSetSize + "\t" +
                                    nodeDoms.toIntString()
                            );
                        }
                    }
                }
            }
        }
    }
    
    private final static String COMMAND =
            "Usage:\n" +
            "  <eq-file>\n" +
            "  <term-file>\n" +
            "  <local-domain-file>\n" +
            "  <strong-domain-file>\n" +
            "  <output-dir>";
    
    private final static Logger LOGGER = Logger
            .getLogger(ExportStrongDomains.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 5) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        File termFile = new File(args[1]);
        File localDomainFile = new File(args[3]);
        File strongDomainFile = new File(args[3]);
        File outputDir = new File(args[4]);

        try {
            new ExportStrongDomains()
                    .run(
                            eqFile,
                            termFile,
                            localDomainFile,
                            strongDomainFile,
                            outputDir
                    );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
