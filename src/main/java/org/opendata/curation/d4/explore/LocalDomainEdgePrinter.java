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
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.graph.ConnectedComponentGenerator;
import org.opendata.core.graph.DirectedConnectedComponents;
import org.opendata.core.graph.UndirectedConnectedComponents;
import org.opendata.curation.d4.Constants;
import org.opendata.curation.d4.column.ExpandedColumn;
import org.opendata.curation.d4.column.ExpandedColumnReader;
import org.opendata.core.io.FileSystem;
import org.opendata.core.object.Entity;
import org.opendata.core.set.EntitySet;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.IdentifiableIDSet;
import org.opendata.core.similarity.JaccardIndex;
import org.opendata.core.util.FormatedBigDecimal;
import org.opendata.curation.d4.domain.EdgeType;
import org.opendata.curation.d4.signature.SignatureBlocks;
import org.opendata.curation.d4.signature.SignatureBlocksConsumer;
import org.opendata.curation.d4.signature.SignatureBlocksGenerator;
import org.opendata.curation.d4.signature.trim.LiberalTrimmer;
import org.opendata.curation.d4.signature.trim.SignatureTrimmerFactory;
import org.opendata.curation.d4.signature.trim.TrimmerType;
import org.opendata.db.eq.EQIndex;

/**
 * Generator for local domains using undirected graphs. Each connected component
 * in the graph generated from the robust signatures of the column elements 
 * represents a local domain.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class LocalDomainEdgePrinter {
    
    private class DomainGenerator implements SignatureBlocksConsumer {

        private final ExpandedColumn _column;
        private final ConnectedComponentGenerator _compGen;
        private final EQIndex _eqIndex;
        private final EntitySet _terms;
        private final PrintWriter _out;
        
        public DomainGenerator(
                EQIndex eqIndex,
                EntitySet terms,
                ExpandedColumn column,
                ConnectedComponentGenerator compGen,
                PrintWriter out
        ) {
            _eqIndex = eqIndex;
            _terms = terms;
            _column = column;
            _compGen = compGen;
            _out = out;
        }
        
        @Override
        public void close() {

        }

        @Override
        public synchronized void consume(SignatureBlocks sig) {

            final int sigId = sig.id();
            
            if (_column.contains(sigId)) {
                HashIDSet edges = new HashIDSet();
                for (int iBlock = 0; iBlock < sig.size(); iBlock++) {
                    for (int nodeId : sig.get(iBlock)) {
                        if (_column.contains(nodeId)) {
                            this.printEdges(sigId, nodeId);
                            edges.add(nodeId);
                        }
                    }
                }
                _compGen.add(sigId, edges.toArray());
            }
        }

        private void printEdges(int nodeI, int nodeJ) {
            
            FormatedBigDecimal sim;
            
            final IDSet colsI = _eqIndex.get(nodeI).columns();
            final IDSet colsJ = _eqIndex.get(nodeJ).columns();
            
            int overlap =  colsI.overlap(colsJ);
            if (overlap > 0) {
               BigDecimal ji = JaccardIndex
                       .ji(colsI.length(), colsJ.length(), overlap);
               sim = new FormatedBigDecimal(ji);
            } else {
                sim = new FormatedBigDecimal(BigDecimal.ZERO);
            }
            
            for (int termI : _eqIndex.get(nodeI).terms()) {
                Entity term = _terms.get(termI);
                for (int termJ : _eqIndex.get(nodeJ).terms()) {
                    String line = term.name() + "\t" + _terms.get(termJ).name() + "\t" + sim;
                    _out.println(line);
                    System.out.println(line);
                }
            }
        }
        
        @Override
        public void open() {

        }
    }
    
    public void run(
            EQIndex eqIndex,
            EntitySet terms,
            TrimmerType trimmer,
            ExpandedColumn column,
            EdgeType edgeType,
            PrintWriter out
    ) throws java.lang.InterruptedException, java.io.IOException {

        ConnectedComponentGenerator domGen;
        switch (edgeType) {
            case Directed:
                domGen = new DirectedConnectedComponents(column.nodes());
                break;
            case Single:
                domGen = new UndirectedConnectedComponents(column.nodes());
                break;
            default:
                throw new IllegalArgumentException("Unknown edge type: " + edgeType.toString());
        }

        SignatureBlocksConsumer consumer;
        consumer = new DomainGenerator(eqIndex, terms, column, domGen, out);

        SignatureTrimmerFactory trimmerFactory;
        trimmerFactory = new SignatureTrimmerFactory(eqIndex, trimmer);
        consumer = trimmerFactory.getTrimmer(column.nodes(), consumer);
        if (!trimmer.equals(TrimmerType.LIBERAL)) {
            consumer = new LiberalTrimmer(eqIndex.nodeSizes(), consumer);
        }
        System.out.println("EDGES\n");
        
        ConcurrentLinkedQueue<Integer> queue;
        queue = new ConcurrentLinkedQueue<>(column.nodes().toList());
        new SignatureBlocksGenerator()
                .runWithMaxDrop(eqIndex, queue, false, true, 6, consumer);
        
        System.out.println("DOMAINS\n");
        
        for (IdentifiableIDSet domain : domGen.getComponents()) {
            System.out.println("DOMAIN " + domain.id() + "\n");
            ArrayList<String> names = new ArrayList<>();
            for (int nodeId : domain) {
                for (int termId : eqIndex.get(nodeId).terms()) {
                    names.add(terms.get(termId).name());
                }
            }
            Collections.sort(names);
            for (String name : names) {
                System.out.println(name);
            }
        }
    }
    
    private static final String COMMAND =
            "Usage\n" +
            "  <eq-file>\n" +
            "  <term-file>\n" +
            "  <signature-trimmer>\n" +
            "  <columns-file>\n" +
            "  <column-id>\n" +
            "  <edge-type>\n" +
            "  <output-file>";
    
    private static final Logger LOGGER = Logger
            .getLogger(LocalDomainEdgePrinter.class.getName());
    
    public static void main(String[] args) {
        
	System.out.println(Constants.NAME + " - Local Domain Edge Printer - Version (" + Constants.VERSION + ")\n");

        if (args.length != 7) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        File termFile = new File(args[1]);
        TrimmerType trimmer = TrimmerType.valueOf(args[2]);
        File columnsFile = new File(args[3]);
        int columnId = Integer.parseInt(args[4]);
        EdgeType edgeType = EdgeType.valueOf(args[5]);
        File outputFile = new File(args[6]);

        FileSystem.createParentFolder(outputFile);
        
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            // Read the node index and the list of columns.
            EQIndex eqIndex = new EQIndex(eqFile);
            // Read the expanded column.
            ExpandedColumn column;
            column = new ExpandedColumnReader(columnsFile).read().get(columnId);
            // Read all entities for the column.
            EntitySet terms = new EntitySet(termFile, eqIndex, column.nodes());
            new LocalDomainEdgePrinter().run(
                    eqIndex,
                    terms,
                    trimmer,
                    column,
                    edgeType,
                    out
            );
        } catch (java.lang.InterruptedException | java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
