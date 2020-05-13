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
import org.opendata.core.set.HashObjectSet;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.core.set.MutableIdentifiableIDSet;
import org.opendata.core.util.count.IdentifiableCounterSet;
import org.opendata.curation.d4.column.ExpandedColumn;
import org.opendata.curation.d4.column.ExpandedColumnIndex;
import org.opendata.curation.d4.column.ExpandedColumnReader;
import org.opendata.curation.d4.domain.Domain;
import org.opendata.curation.d4.domain.DomainReader;
import org.opendata.curation.d4.domain.StrongDomain;
import org.opendata.curation.d4.domain.StrongDomainReader;
import org.opendata.db.eq.EQIndex;

/**
 * Print identifier of nodes that are not assigned to a strong domain in a
 * clean column. A column is considered clean if less than n column values are
 * not assigned to a strong domain.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class CleanColumnOutliers {
 
    private final static String COMMAND =
            "Usage:\n" +
            "  <eq-index>\n" +
            "  <expanded-column-file>\n" +
            "  <local-domain-file>\n" +
            "  <strong-domain-file>\n" +
            "  <n>\n" +
            "  <output-file>";
    
    private final static Logger LOGGER = Logger
            .getLogger(CleanColumnOutliers.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 6) {
            System.out.println(COMMAND);
            System.exit(-1);
        }

        File eqFile = new File(args[0]);
        File expandedColumnFile = new File(args[1]);
        File localDomainFile = new File(args[2]);
        File strongDomainFile = new File(args[3]);
        int n = Integer.parseInt(args[4]);
        File outputFile = new File(args[5]);
        
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            // Read equivalence classes
            EQIndex eqIndex = new EQIndex(eqFile);
            // Read expanded columns
            ExpandedColumnIndex columnIndex = new ExpandedColumnIndex();
            new ExpandedColumnReader(expandedColumnFile).stream(columnIndex);            
            // Read local domains
            IdentifiableObjectSet<Domain> localDomains;
            localDomains = new DomainReader(localDomainFile).read();
            // Read strong domains
            IdentifiableObjectSet<StrongDomain> strongDomains;
            strongDomains = new StrongDomainReader(strongDomainFile).read();
            // Column nodes in local and strong domains
            HashObjectSet<MutableIdentifiableIDSet> locDomNodes = new HashObjectSet<>();
            HashObjectSet<MutableIdentifiableIDSet> strongDomNodes = new HashObjectSet<>();
            for (Domain localDomain : localDomains) {
                HashObjectSet<MutableIdentifiableIDSet> columns = null;
                for (StrongDomain strongDomain : strongDomains) {
                    if (strongDomain.localDomains().contains(localDomain.id())) {
                        columns = strongDomNodes;
                        break;
                    }
                }
                if (columns == null) {
                    columns = locDomNodes;
                }
                for (int columnId : localDomain.columns()) {
                    if (columns.contains(columnId)) {
                        columns.get(columnId).add(localDomain);
                    } else {
                        columns.add(new MutableIdentifiableIDSet(columnId, localDomain));
                    }
                }
            }
            // Clean column outliers
            IdentifiableCounterSet counts = new IdentifiableCounterSet();
            for (ExpandedColumn column : columnIndex.columns()) {
                if (strongDomNodes.contains(column.id())) {
                    IDSet outliers = column.nodes().difference(strongDomNodes.get(column.id()));
                    if (outliers.length() < n) {
                        for (int nodeId : outliers) {
                            counts.inc(nodeId);
                        }
                    }
                }
            }
            // Write outlier candidate stats.
            for (int nodeId : counts.keys()) {
                int candCount = counts.get(nodeId).value();
                if (candCount < 2) {
                    continue;
                }
                int[] stats = new int[]{candCount, 0, 0, 0};
                for (int columnId : eqIndex.get(nodeId).columns()) {
                    int index = 1;
                    if (locDomNodes.contains(columnId)) {
                        if (locDomNodes.get(columnId).contains(nodeId)) {
                            index = 2;
                        }
                    }
                    if (index == 1) {
                        if (strongDomNodes.contains(columnId)) {
                            if (strongDomNodes.get(columnId).contains(nodeId)) {
                                index = 3;
                            }
                        }
                    }
                    stats[index] += 1;
                }
                out.println(nodeId + "\t" + stats[0] + "\t" + stats[1] + "\t" + stats[2] + "\t" + stats[3]);
            }
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
