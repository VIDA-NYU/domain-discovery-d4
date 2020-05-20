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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.curation.d4.domain.Domain;
import org.opendata.curation.d4.domain.DomainReader;
import org.opendata.curation.d4.domain.StrongDomain;
import org.opendata.curation.d4.domain.StrongDomainReader;

/**
 * Print information about a set of local and strong domains.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DomainStatsPrinter {
 
    private final static String COMMAND =
            "Usage:\n" +
            "  <local-domain-file>\n" +
            "  <strong-domain-file>";
    
    private final static Logger LOGGER = Logger
            .getLogger(DomainStatsPrinter.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 2) {
            System.out.println(COMMAND);
            System.exit(-1);
        }

        File localDomainFile = new File(args[0]);
        File strongDomainFile = new File(args[1]);
        
        try {
            // Print information about local domains
            IdentifiableObjectSet<Domain> localDomains;
            localDomains = new DomainReader(localDomainFile).read();
            HashIDSet locDomNodes = new HashIDSet();
            HashIDSet locDomCols = new HashIDSet();
            for (Domain domain : localDomains) {
                locDomNodes.add(domain);
                locDomCols.add(domain.columns());
            }
            System.out.println("LOCAL DOMAINS");
            System.out.println("-------------");
            System.out.println("NUMBER OF DOMAINS   : " + localDomains.length());
            System.out.println("EQs IN DOMAINS      : " + locDomNodes.length());
            System.out.println("COLUMNS WITH DOMAINS: " + locDomCols.length());
            // Read strong domains
            IdentifiableObjectSet<StrongDomain> strongDomains;
            strongDomains = new StrongDomainReader(strongDomainFile).read();
            HashIDSet strongDomNodes = new HashIDSet();
            HashIDSet strongDomCols = new HashIDSet();
            for (StrongDomain domain : strongDomains) {
                strongDomNodes.add(domain.members().keys());
                strongDomCols.add(domain.columns());
            }
            System.out.println("STRONG DOMAINS");
            System.out.println("--------------");
            System.out.println("NUMBER OF DOMAINS   : " + strongDomains.length());
            System.out.println("EQs IN DOMAINS      : " + strongDomNodes.length());
            System.out.println("COLUMNS WITH DOMAINS: " + strongDomCols.length());
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
