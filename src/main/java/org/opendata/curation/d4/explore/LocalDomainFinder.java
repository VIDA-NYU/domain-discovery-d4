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
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.constraint.Threshold;
import org.opendata.core.io.EntitySetReader;
import org.opendata.core.io.FileSystem;
import org.opendata.core.set.EntitySet;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.HashObjectSet;
import org.opendata.core.set.IdentifiableIDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.core.set.MutableIdentifiableIDSet;
import org.opendata.curation.d4.column.ExpandedColumn;
import org.opendata.curation.d4.column.ExpandedColumnIndex;
import org.opendata.curation.d4.column.MutableExpandedColumn;
import org.opendata.curation.d4.column.SingleColumnExpander;
import org.opendata.curation.d4.column.SupportCounter;
import org.opendata.curation.d4.domain.Domain;
import org.opendata.curation.d4.domain.UndirectedDomainGenerator;
import org.opendata.curation.d4.domain.UniqueDomainSet;
import org.opendata.curation.d4.signature.RobustSignatureConsumer;
import org.opendata.curation.d4.signature.RobustSignatureGenerator;
import org.opendata.curation.d4.signature.SignatureBlock;
import org.opendata.curation.d4.signature.SignatureBlocksIndex;
import org.opendata.curation.d4.signature.trim.SignatureTrimmer;
import org.opendata.curation.d4.signature.trim.SignatureTrimmerFactory;
import org.opendata.db.eq.EQIndex;

/**
 * Find local domains for a given column. Allows to provide parameters for
 * different steps of the algorithm. Maintains and prints provenance information
 * about the discovery process.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class LocalDomainFinder {
   
    private static final String COMMAND = 
            "Usage:\n" +
            "  <eq-file>\n" +
            "  <term-file>\n" +
            "  <column-id>\n" +
            "-- Signatures\n" +
            "  <robustifier>\n" +
            "  <full-signature-constraint>\n" +
            "  <ignore-last-drop>\n" +
            "  <ignore-minor-drops>\n" +
            "-- Column expansion\n" +
            "  <expand-timmer>\n" +
            "  <support-threshold>\n" +
            "-- Local domains\n" +
            "  <domain-timmer>\n" +
            "-- General\n" +
            "  <threads>\n" +
            "  <output-file>";
    
    private static final Logger LOGGER = Logger
            .getLogger(LocalDomainFinder.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 12) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        // Input files.
        File eqFile = new File(args[0]);
        File termFile = new File(args[1]);
        int columnId = Integer.parseInt(args[2]);
        
        // Signature parameters.
        String robustifierSpec = args[3].toUpperCase();
        boolean fullSignatureConstraint = Boolean.parseBoolean(args[4]);
        boolean ignoreLastDrop = Boolean.parseBoolean(args[5]);
        boolean ignoreMinorDrops = Boolean.parseBoolean(args[6]);
        
        // Column expansion.
        String expandTrimmerSpec = args[7].toUpperCase();
        Threshold threshold = Threshold.getConstraint(args[8]);
        
        // Local domains.
        String domainTrimmerSpec = args[9].toUpperCase();
        
        // General.
        int threads = Integer.parseInt(args[10]);
        File outputFile = new File(args[11]);
        
        /*
         * Get equivalence class index and target column.
         */
        
        EQIndex eqIndex = null;
        try {
            eqIndex = new EQIndex(eqFile);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "EQ", ex);
            System.exit(-1);
        }
        
        IdentifiableObjectSet<IdentifiableIDSet> columns = eqIndex.columns();
        IdentifiableIDSet column = columns.get(columnId);
        
        /*
        * Signature blocks
        */
        SignatureBlocksIndex signatures = new SignatureBlocksIndex();
        try {
            new RobustSignatureGenerator().run(
                    eqIndex,
                    new ConcurrentLinkedQueue<>(column.toList()),
                    robustifierSpec,
                    fullSignatureConstraint,
                    ignoreLastDrop,
                    ignoreMinorDrops,
                    threads,
                    true,
                    signatures
            );
        } catch (java.lang.InterruptedException ex) {
            LOGGER.log(Level.SEVERE, "SIGNATURES", ex);
        }
        
        /*
         * Expanded Column
         */
        ExpandedColumn expandedColumn = new MutableExpandedColumn(column);
        SingleColumnExpander columnExpander = new SingleColumnExpander(
                eqIndex,
                expandedColumn,
                threshold,
                BigDecimal.ZERO,
                1
        );
        SignatureTrimmer expandTrimmer;
        expandTrimmer = new SignatureTrimmerFactory(eqIndex, columns, expandTrimmerSpec)
                .getTrimmer(expandedColumn.id(),columnExpander);
        
        signatures.stream(expandTrimmer);
        
        expandedColumn = columnExpander.column();
        
        System.out.println("EXPANDED WITH " + expandedColumn.expandedNodes());
        
        /*
         * Local Domains
         */
        
        // Compute signatures for nodes in the expansion set.
        if (!expandedColumn.expandedNodes().isEmpty()) {
            try {
                new RobustSignatureGenerator().run(
                        eqIndex,
                        new ConcurrentLinkedQueue<>(expandedColumn.expandedNodes().toList()),
                        robustifierSpec,
                        fullSignatureConstraint,
                        ignoreLastDrop,
                        ignoreMinorDrops,
                        threads,
                        true,
                        signatures
                );
            } catch (java.lang.InterruptedException ex) {
                LOGGER.log(Level.SEVERE, "EXPANSION SIGNATURES", ex);
            }
        }

        UniqueDomainSet domainSet;
        domainSet = new UniqueDomainSet(new ExpandedColumnIndex(expandedColumn));
        
        RobustSignatureConsumer domainGenerator;
        domainGenerator = new UndirectedDomainGenerator(
                expandedColumn,
                domainSet,
                eqIndex.nodeSizes()
        );
        
        HashObjectSet<IdentifiableIDSet> expandedColumnSet = new HashObjectSet<>();
        expandedColumnSet.add(new MutableIdentifiableIDSet(columnId, expandedColumn.nodes()));
        
        SignatureTrimmer domainTrimmer;
        domainTrimmer = new SignatureTrimmerFactory(eqIndex, expandedColumnSet, domainTrimmerSpec)
                .getTrimmer(expandedColumn.id(), domainGenerator);

        signatures.stream(domainTrimmer);
        
        List<Domain> domains = domainSet.domains();
        
        /*
         * Output
         */
        
        // Collect representative terms for all equivalence classes.
        HashIDSet nodes = new HashIDSet(expandedColumn.nodes());
        for (int nodeId : signatures.keys()) {
            for (SignatureBlock block : signatures.get(nodeId)) {
                for (int memberId : block.elements()) {
                    nodes.add(memberId);
                }
            }
        }
        nodes.union(new HashIDSet(columnExpander.support().keySet()));
        
        System.out.println("Read terms for " + nodes.length() + " equivalence classes");
        HashMap<Integer, Integer> mapping = new HashMap<>();
        for (int nodeId : nodes) {
            int termId = eqIndex.get(nodeId).terms().first();
            mapping.put(nodeId, termId);
        }
        HashIDSet termFilter = new HashIDSet(mapping.values());
        EntitySet terms = null;
        try {
            terms = new EntitySetReader(termFile).readEntities(termFilter);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "READ TERMS", ex);
            System.exit(-1);
        }

        int columnSize = 0;
        for (int nodeId : column) {
            columnSize += eqIndex.nodeSizes()[nodeId];
        }
        
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            out.println("COLUMN");
            for (int nodeId : column) {
                out.println("\t" + terms.get(mapping.get(nodeId)));
                for (SignatureBlock block : signatures.get(nodeId)) {
                    out.println(String.format("\t\t%.6f-%.6f", block.firstValue(), block.lastValue()));
                    for (int memberId : block.elements()) {
                        out.println("\t\t\t" + terms.get(mapping.get(memberId)));
                    }
                }
            }
            out.println("EXPANSION");
            for (int nodeId : columnExpander.support().keySet()) {
                SupportCounter sup = columnExpander.support().get(nodeId);
                String name = terms.get(mapping.get(nodeId)).name();
                if (expandedColumn.expandedNodes().contains(nodeId)) {
                    name += " (*)";
                }
                out.println(String.format("\t\t%d\t%.6f\t%s", sup.originalSupportCount(), sup.originalSupport(columnSize), name));
            }
            out.println("DOMAINS");
            for (Domain domain : domains) {
                out.println("\tDOMAIN");
                for (int nodeId : domain) {
                    out.println("\t\t" + terms.get(mapping.get(nodeId)));
                }
            }
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "WRITER", ex);
            System.exit(-1);
        }
    }
}
