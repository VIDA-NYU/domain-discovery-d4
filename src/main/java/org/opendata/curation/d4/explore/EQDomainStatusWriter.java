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
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.FileSystem;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.curation.d4.column.ExpandedColumn;
import org.opendata.curation.d4.column.ExpandedColumnIndex;
import org.opendata.curation.d4.column.ExpandedColumnReader;
import org.opendata.curation.d4.domain.Domain;
import org.opendata.curation.d4.domain.DomainReader;
import org.opendata.curation.d4.domain.StrongDomain;
import org.opendata.curation.d4.domain.StrongDomainReader;

/**
 * Write domain allocation information about every EQ in every column. Writes
 * a tab-delimited file with three columns:
 * 
 * - column-id
 * - eq-id
 * - domain-status
 * 
 * The domain status is represented by one of three values:
 * 
 * '-': Not assigned to any domain
 * 'L': Assigned to a local domain
 * 'S': Assigned to a strong domain for that column
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EQDomainStatusWriter {
 
    private final static String COMMAND =
            "Usage:\n" +
            "  <expanded-column-file>\n" +
            "  <local-domain-file>\n" +
            "  <strong-domain-file>\n" +
            "  <output-file>";
    
    private final static Logger LOGGER = Logger
            .getLogger(EQDomainStatusWriter.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 4) {
            System.out.println(COMMAND);
            System.exit(-1);
        }

        File expandedColumnFile = new File(args[0]);
        File localDomainFile = new File(args[1]);
        File strongDomainFile = new File(args[2]);
        File outputFile = new File(args[3]);
        
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            // Read expanded columns
            ExpandedColumnIndex columnIndex = new ExpandedColumnIndex();
            new ExpandedColumnReader(expandedColumnFile).stream(columnIndex);            
            // Read local domains
            IdentifiableObjectSet<Domain> localDomains;
            localDomains = new DomainReader(localDomainFile).read();
            // Read strong domains
            IdentifiableObjectSet<StrongDomain> strongDomains;
            strongDomains = new StrongDomainReader(strongDomainFile).read();
            // Write EQ domain status
            for (ExpandedColumn column : columnIndex.columns()) {
                List<Domain> locDoms = new ArrayList<>();
                List<Domain> strongDoms = new ArrayList<>();
                for (Domain localDomain : localDomains) {
                    if (localDomain.columns().contains(column.id())) {
                        boolean isStrong = false;
                        for (StrongDomain strongDomain : strongDomains) {
                            if (strongDomain.localDomains().contains(localDomain.id())) {
                                strongDoms.add(localDomain);
                                isStrong = true;
                                break;
                            }
                        }
                        if (!isStrong) {
                            locDoms.add(localDomain);
                        }
                    }
                }
                for (int nodeId :  column.nodes()) {
                    char status = '-';
                    for (Domain domain : strongDoms) {
                        if (domain.contains(nodeId)) {
                            status = 'S';
                            break;
                        }
                    }
                    if (status == '-') {
                        for (Domain domain : locDoms) {
                            if (domain.contains(nodeId)) {
                                status = 'L';
                                break;
                            }
                        }
                    }
                    out.println(column.id() + "\t" + nodeId + "\t" + status);
                }
            }
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
