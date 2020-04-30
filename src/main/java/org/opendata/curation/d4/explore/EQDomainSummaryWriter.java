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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.FileSystem;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.core.util.StringHelper;
import org.opendata.core.util.count.IdentifiableCounterSet;
import org.opendata.curation.d4.column.ExpandedColumn;
import org.opendata.curation.d4.column.ExpandedColumnIndex;
import org.opendata.curation.d4.column.ExpandedColumnReader;
import org.opendata.curation.d4.domain.Domain;
import org.opendata.curation.d4.domain.DomainReader;
import org.opendata.curation.d4.domain.StrongDomain;
import org.opendata.curation.d4.domain.StrongDomainMember;
import org.opendata.curation.d4.domain.StrongDomainReader;
import org.opendata.db.eq.EQ;
import org.opendata.db.eq.EQIndex;

/**
 * Create summary for each equivalence class about the number of local domains
 * and strong domains. Writes a tab-delimited file with five columns:
 * 
 * 1) EQ identifier
 * 2) Number of columns the EQ occurs in
 * 3) Number of columns the EQ was added to be expansion
 * 4) Number of local domains the EQ occurs in
 * 5) Number of strong domains the EQ occurs in
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EQDomainSummaryWriter {
 
    private final static String COMMAND =
            "Usage:\n" +
            "  <eq-file>\n" +
            "  <expanded-column-file>\n" +
            "  <local-domain-file>\n" +
            "  <strong-domain-file>\n" +
            "  <output-file>";
    
    private final static Logger LOGGER = Logger
            .getLogger(EQDomainSummaryWriter.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 5) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        File expandedColumnFile = new File(args[1]);
        File localDomainFile = new File(args[2]);
        File strongDomainFile = new File(args[3]);
        File outputFile = new File(args[4]);
        
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            // Read equivalence classes
            EQIndex nodes = new EQIndex(eqFile);
            // Read expanded columns
            ExpandedColumnIndex columnIndex = new ExpandedColumnIndex();
            new ExpandedColumnReader(expandedColumnFile).stream(columnIndex);            
            // Read local domains
            IdentifiableObjectSet<Domain> localDomains;
            localDomains = new DomainReader(localDomainFile).read();
            // Read strong domains
            IdentifiableObjectSet<StrongDomain> strongDomains;
            strongDomains = new StrongDomainReader(strongDomainFile).read();
            // Write EQ stats
            IdentifiableCounterSet expColCount = new IdentifiableCounterSet();
            IdentifiableCounterSet locDomCount = new IdentifiableCounterSet();
            IdentifiableCounterSet strongDomCount = new IdentifiableCounterSet();
            for (ExpandedColumn column : columnIndex.columns()) {
                for (int nodeId :  column.expandedNodes()) {
                    expColCount.inc(nodeId);
                }
            }
            for (Domain localDomain : localDomains) {
                for (int nodeId : localDomain.nodes()) {
                    locDomCount.inc(nodeId);
                }
            }
            for (StrongDomain strongDomain : strongDomains) {
                for (StrongDomainMember member : strongDomain.members()) {
                    strongDomCount.inc(member.id());
                }
            }
            for (EQ node : nodes) {
                int[] values = new int[]{
                    node.id(),
                    node.columns().length(),
                    expColCount.get(node.id()).value(),
                    locDomCount.get(node.id()).value(),
                    strongDomCount.get(node.id()).value()
                };
                out.println(StringHelper.joinIntegers(values, "\t"));
            }
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
