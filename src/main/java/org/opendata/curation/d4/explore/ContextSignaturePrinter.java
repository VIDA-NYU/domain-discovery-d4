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
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.curation.d4.Arguments;
import org.opendata.curation.d4.Constants;
import org.opendata.core.constraint.GreaterThanConstraint;
import org.opendata.core.prune.MaxDropFinder;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.curation.d4.signature.ContextSignatureGenerator;
import org.opendata.curation.d4.signature.SignatureValue;
import org.opendata.db.eq.EQIndex;
import org.opendata.db.term.Term;
import org.opendata.db.term.TermIndexReader;

/**
 * Print context signature for node.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ContextSignaturePrinter {
    
    public void print(
            EQIndex eqIndex,
            TermIndexReader termReader,
            boolean fullSignatureConstraint,
            boolean ignoreLastDrop,
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
        IdentifiableObjectSet<Term> termIndex = termReader.read(nodeFilter);

        int start = 0;
        final int end = sig.size();
        int blockCount = 0;
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
            Arrays.sort(block);
            String headline = "\n-- BLOCK " + blockCount + " (" + nodeCount + " NODES, " + termCount + " TERMS)";
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
    }
    
    private static final String ARG_FULLSIG = "fullSigConstraint";
    private static final String ARG_LASTDROP = "ignoreLastDrop";
    
    private static final String[] ARGS = {
        ARG_FULLSIG,
        ARG_LASTDROP
    };
    
    private static final String COMMAND =
            "Usage\n" +
            "  --" + ARG_FULLSIG + "=[true | false] [default: false]\n" +
            "  --" + ARG_LASTDROP + "=[true | false] [default: true]\n" +
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
        
        try {
            // Read the node index
            EQIndex nodeIndex = new EQIndex(eqFile);
            new ContextSignaturePrinter().print(
                    nodeIndex,
                    new TermIndexReader(termFile),
                    fullSignatureConstraint,
                    ignoreLastDrop,
                    nodeId
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
