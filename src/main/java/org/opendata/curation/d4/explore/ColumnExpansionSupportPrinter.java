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
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.curation.d4.signature.SignatureBlocks;
import org.opendata.curation.d4.signature.SignatureBlocksConsumer;
import org.opendata.core.util.count.Counter;
import org.opendata.curation.d4.column.ExpandedColumn;
import org.opendata.curation.d4.column.ImmutableExpandedColumn;
import org.opendata.curation.d4.signature.SignatureBlocksGenerator;
import org.opendata.curation.d4.signature.trim.LiberalTrimmer;
import org.opendata.curation.d4.signature.trim.SignatureTrimmer;
import org.opendata.curation.d4.signature.trim.SignatureTrimmerFactory;
import org.opendata.curation.d4.signature.trim.TrimmerType;
import org.opendata.db.Database;
import org.opendata.db.column.Column;
import org.opendata.db.eq.EQIndex;

/**
 * Simplified version of the single column expander. This expander does only a
 * single round of expansion.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnExpansionSupportPrinter implements SignatureBlocksConsumer {

    private static final Logger LOGGER = Logger
            .getLogger(ColumnExpansionSupportPrinter.class.getName());
        
    private ExpandedColumn _column = null;
    private final int[] _nodeSizes;
    private HashMap<Integer, Counter> _support;
    
    public ColumnExpansionSupportPrinter(
            ExpandedColumn column,
            int[] nodeSizes
    ) {
        _nodeSizes = nodeSizes;
        _column = column;
    }
    
    @Override
    public void close() {

        System.out.println("\nNODE SUPPORT\n");
    
        for (int nodeId : _support.keySet()) {
            System.out.println(nodeId + " = " + _support.get(nodeId).value());
        }
    }

    @Override
    public void consume(SignatureBlocks sig) {

        if (_column.isColumnNode(sig.id())) {
            int weight = _nodeSizes[sig.id()];
            for (int iBlock = 0; iBlock < sig.size(); iBlock++) {
                int[] block = sig.get(iBlock);
                System.out.println(sig.id() + " SUPPORTS BLOCK OF " + block.length);
                for (int nodeId : block) {
                    if (!_column.contains(nodeId)) {
                        if (!_support.containsKey(nodeId)) {
                            _support.put(nodeId, new Counter(weight));
                        } else {
                            Counter counter = _support.get(nodeId);
                            counter.inc(weight);
                        }
                    }
                }
            }
       }
    }

    @Override
    public void open() {

        _support = new HashMap<>();
    }
    
    private final static String COMMAND =
            "Usage:\n" +
            "  <eq-file>\n" +
            "  <trimmer-type>\n" +
            "  <column-id>";
    
    public static void main(String[] args) {
        
        if (args.length != 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        TrimmerType trimmer = TrimmerType.valueOf(args[1]);
        int columnId = Integer.parseInt(args[2]);
        
        try {
            EQIndex eqIndex = new EQIndex(eqFile);
            int[] nodeSizes = eqIndex.nodeSizes();
            Column column = new Database(eqIndex).columns().get(columnId);
            ColumnExpansionSupportPrinter expander;
            expander = new ColumnExpansionSupportPrinter(
                    new ImmutableExpandedColumn(column),
                    nodeSizes
            );
            SignatureTrimmer consumer;
            consumer = new SignatureTrimmerFactory(nodeSizes, trimmer)
                    .getTrimmer(column, expander);
            if (!trimmer.equals(TrimmerType.LIBERAL)) {
                consumer = new LiberalTrimmer(eqIndex.nodeSizes(), consumer);
            }
            ConcurrentLinkedQueue<Integer> queue;
            queue = new ConcurrentLinkedQueue<>(column.toList());
            new SignatureBlocksGenerator()
                    .runWithMaxDrop(eqIndex, queue, false, true, 6, consumer);
        } catch (java.lang.InterruptedException | java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
