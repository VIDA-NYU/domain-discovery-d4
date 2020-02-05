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
package org.opendata.curation.d4.signature;

import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.curation.d4.Arguments;
import org.opendata.curation.d4.Constants;
import org.opendata.curation.d4.signature.trim.CentristTrimmer;
import org.opendata.curation.d4.signature.trim.LiberalTrimmer;
import org.opendata.curation.d4.signature.trim.PrecisionScore;
import org.opendata.core.constraint.GreaterThanConstraint;
import org.opendata.core.object.IdentifiableDouble;
import org.opendata.core.prune.MaxDropFinder;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.db.column.Column;
import org.opendata.db.eq.EQIndex;
import org.opendata.db.term.Term;
import org.opendata.db.term.TermIndexReader;

/**
 * Print context signature for node. Allows to include scores of blocks for a 
 * given column.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ContextSignaturePrinter {
    
    public void print(
            EQIndex eqIndex,
            TermIndexReader termReader,
            boolean fullSignatureConstraint,
            boolean ignoreLastDrop,
            Column column,
            int nodeId
    ) throws java.io.IOException {

        MaxDropFinder<SignatureValue> candidateFinder;
        candidateFinder = new MaxDropFinder<>(
                new GreaterThanConstraint(BigDecimal.ZERO),
                fullSignatureConstraint,
                ignoreLastDrop
        );

        List<SignatureValue> sig;
        sig = new ContextSignatureGenerator(eqIndex.nodes())
                .getSignature(nodeId)
                .rankedElements();
        
        HashIDSet nodeFilter = new HashIDSet();
        for (SignatureValue el : sig) {
            nodeFilter.add(eqIndex.get(el.id()).terms());
        }
        if (column != null) {
            for (int n : column) {
                nodeFilter.add(eqIndex.get(n).terms());
            }
        }
        IdentifiableObjectSet<Term> termIndex = termReader.read(nodeFilter);

        if (column != null) {
            System.out.println("COLUMN");
            for (int n : column) {
                for (int termId : eqIndex.get(n).terms()) {
                    String value = termIndex.get(termId).name();
                    System.out.println(n + "\t" + termId + "\t" + value);
                }
            }
            System.out.println();
        }
        
        int[] columnNodes = null;
        int columnSize = -1;
        int[] nodeSizes = null;
        List<IdentifiableDouble> scores = null;
        if (column != null) {
            columnNodes = column.toArray();
            nodeSizes = eqIndex.nodeSizes();
            for (int n : column) {
                columnSize += eqIndex.get(n).terms().length();
            }
            scores = new ArrayList<>();
        }
        int start = 0;
        final int end = sig.size();
        int blockCount = 0;
        ArrayList<int[]> blocks = new ArrayList<>();
        while (start < end) {
            int pruneIndex = candidateFinder.getPruneIndex(sig, start);
            if (pruneIndex <= start) {
                break;
            }
            blockCount++;
            int nodeCount = pruneIndex - start;
            int[] block = new int[nodeCount];
            int termCount = 0;
            for (int iEl = start; iEl < pruneIndex; iEl++) {
                SignatureValue el = sig.get(iEl);
                block[iEl - start] = el.id();
                termCount += eqIndex.get(el.id()).terms().length();
            }
            blocks.add(block);
            Arrays.sort(block);
            if (column != null ) {
                scores.add(
                        this.score(
                                blockCount,
                                block,
                                columnNodes,
                                columnSize,
                                nodeSizes
                        )
                );
            }
            String headline = "\n-- BLOCK " + blockCount + " (" + nodeCount + " NODES, " + termCount + " TERMS)";
            if (column != null) {
                headline += " SCORE " + scores.get(scores.size() - 1).toPlainString() + "\n";
            }
            System.out.println(headline);
            for (int iEl = start; iEl < pruneIndex; iEl++) {
                SignatureValue el = sig.get(iEl);
                boolean isFirst = true;
                for (int termId : eqIndex.get(el.id()).terms()) {
                    String line;
                    if (isFirst) {
                        line = Integer.toString(el.id());
                        isFirst = false;
                    } else {
                        line = "";
                    }
                    line += "\t" + termIndex.get(termId).name() + "\t" + el.toPlainString();
                    System.out.println(line);
                }
            }
            start = pruneIndex;
        }
        
        if (column != null) {
            RobustSignatureIndex buffer = new RobustSignatureIndex();
            new LiberalTrimmer(
                    nodeSizes,
                    new CentristTrimmer(column, nodeSizes, buffer)
            ).consume(new SignatureBlocksImpl(nodeId, BigDecimal.ONE, blocks));
            System.out.println("\nSIGNATURE BLOCKS FOR COLUMN " + column.id() + "\n");
            SignatureBlocks sigBlocks = buffer.get(nodeId);
            for (int iBlock = 0; iBlock < sigBlocks.size(); iBlock++) {
                for (int n : sigBlocks.get(iBlock)) {
                boolean isFirst = true;
                for (int termId : eqIndex.get(n).terms()) {
                    String line;
                    if (isFirst) {
                        line = Integer.toString(n);
                        isFirst = false;
                    } else {
                        line = "";
                    }
                    line += "\t" + termIndex.get(termId).name();
                    System.out.println(line);
                }
                }
                System.out.println();
            }
        }
    }

    private IdentifiableDouble score(
            int blockId,
            int[] block,
            int[] column,
            int columnSize,
            int[] nodeSizes
    ) {
        final int len1 = block.length;
        final int len2 = column.length;
        int idx1 = 0;
        int idx2 = 0;
        int blSize = 0;
        int overlap = 0;
        while ((idx1 < len1) && (idx2 < len2)) {
            final int nodeId = block[idx1];
            int comp = Integer.compare(nodeId, column[idx2]);
            if (comp < 0) {
                blSize += nodeSizes[nodeId];
                idx1++;
            } else if (comp > 0) {
                idx2++;
            } else {
                int nodeSize = nodeSizes[nodeId];
                blSize += nodeSize;
                overlap += nodeSize;
                idx1++;
                idx2++;
            }
        }
        while (idx1 < len1) {
            blSize += nodeSizes[block[idx1++]];
        }
        if (overlap > 0) {
            BigDecimal val = new PrecisionScore().relevance(columnSize, blSize, overlap);
            return new IdentifiableDouble(blockId, val.doubleValue());
        } else {
            return new IdentifiableDouble(blockId, 0.0);
        }
    }
    
    private static final String ARG_COLUMN = "column";
    private static final String ARG_FULLSIG = "fullSigConstraint";
    private static final String ARG_LASTDROP = "ignoreLastDrop";
    
    private static final String[] ARGS = {
        ARG_COLUMN,
        ARG_FULLSIG,
        ARG_LASTDROP
    };
    
    private static final String COMMAND =
            "Usage\n" +
            "  --" + ARG_FULLSIG + "=[true | false] [default: false]\n" +
            "  --" + ARG_LASTDROP + "=[true | false] [default: true]\n" +
            "  --" + ARG_COLUMN + "=<int> (default: none)\n" +
            "  <eq-file>\n" +
            "  <term-file>\n" +
            "  <node-id>";
    
    private static final Logger LOGGER = Logger
            .getLogger(ContextSignaturePrinter.class.getName());
    
    public static void main(String[] args) {
        
        System.out.println(Constants.NAME + " - Context Signature Printer - Version (" + Constants.VERSION + ")\n");

        if (args.length < 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        Arguments params = new Arguments(ARGS, args, 3);
        File eqFile = new File(params.fixedArg(0));
        File termFile = new File(params.fixedArg(1));
        int nodeId = Integer.parseInt(params.fixedArg(2));

        boolean fullSignatureConstraint = params.getAsBool(ARG_FULLSIG, false);
        boolean ignoreLastDrop = params.getAsBool(ARG_LASTDROP, true);
        int columnId = -1;
        if (params.has(ARG_COLUMN)) {
            columnId = params.getAsInt(ARG_COLUMN);
        }
        
        try {
            // Read the node index
            EQIndex nodeIndex = new EQIndex(eqFile);
            Column column = null;
            if (columnId > -1) {
                column = nodeIndex.columns().get(columnId);
            }
            new ContextSignaturePrinter().print(
                    nodeIndex,
                    new TermIndexReader(termFile),
                    fullSignatureConstraint,
                    ignoreLastDrop,
                    column,
                    nodeId
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
