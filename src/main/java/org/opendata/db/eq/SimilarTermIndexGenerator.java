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
package org.opendata.db.eq;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.constraint.Threshold;
import org.opendata.core.graph.ParallelOverlapGraphGenerator;
import org.opendata.core.graph.UndirectedConnectedComponents;
import org.opendata.core.io.FileSystem;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IdentifiableIDSet;
import org.opendata.core.util.StringHelper;
import org.opendata.curation.d4.Constants;

/**
 * Generate a compressed term index where similar equivalence classes are
 * merged.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SimilarTermIndexGenerator {
    
    /**
     * Naive equivalence class merger for similar equivalence classes. Creates
     * an undirected graph where edges between nodes exist based on whether they
     * satisfy the similarity threshold or not. Connected components in this
     * graph identify the sets of equivalence classes that are being merged.
     * 
     * @param eqIndex
     * @param threshold
     * @param threads
     * @param verbose
     * @param out 
     */
    public void runNaive(
            EQIndex eqIndex,
            Threshold threshold,
            int threads,
            boolean verbose,
            PrintWriter out
    ) {
        
        UndirectedConnectedComponents graph;
        graph = new UndirectedConnectedComponents(eqIndex.nodes().keys());
        
        new ParallelOverlapGraphGenerator()
                .run(eqIndex.nodes().toList(), threshold, threads, graph);
        
        for (IdentifiableIDSet comp : graph.getComponents()) {
            if (comp.length() == 1) {
                eqIndex.get(comp.first()).write(out);
            } else {
                HashIDSet columns = new HashIDSet();
                HashIDSet terms = new HashIDSet();
                for (int nodeId : comp) {
                    EQ node = eqIndex.get(nodeId);
                    columns.add(node.columns());
                    terms.add(node.terms());
                }
                new EQImpl(comp.id(), terms, columns).write(out);
                if (verbose) {
                    ArrayList<String> tokens = new ArrayList<>();
                    for (int nodeId : comp) {
                        EQ node = eqIndex.get(nodeId);
                        tokens.add(
                                String.format(
                                        "%d[%d:%d]",
                                        node.id(),
                                        node.columns().length(),
                                        node.terms().length()
                                )
                        );
                    }
                    System.out.println(comp.id() + "\t" + StringHelper.joinStrings(tokens));
                }
            }
        }
    }
    
    private final static String COMMAND =
            "Usage:\n" +
            "  <eq-file>\n" +
            "  <strategy> [NAIVE]\n" +
            "  <threshold>\n" +
            "  <verbose>\n" +
            "  <threads>\n" +
            "  <output-file>";
    
    private final static Logger LOGGER = Logger
            .getLogger(SimilarTermIndexGenerator.class.getName());
    
    public static void main(String[] args) {
        
        System.out.println(Constants.NAME + " - Similar Term Index Generator - Version (" + Constants.VERSION + ")\n");

        if (args.length != 6) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        String strategy = args[1].toUpperCase();
        Threshold threshold = Threshold.getConstraint(args[2]);
        boolean verbose = Boolean.parseBoolean(args[3]);
        int threads = Integer.parseInt(args[4]);
        File outFile = new File(args[5]);
        
        try (PrintWriter out = FileSystem.openPrintWriter(outFile)) {
            if (strategy.equals("NAIVE")) {
                new SimilarTermIndexGenerator()
                        .runNaive(
                                new EQIndex(eqFile),
                                threshold,
                                threads,
                                verbose,
                                out
                        );
            } else {
                throw new IllegalArgumentException(
                        String.format("Unknown strategy '%s'", strategy)
                );
            }
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
