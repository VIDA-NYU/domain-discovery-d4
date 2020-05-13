/*
 * Copyright 2018 New York University.
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
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.constraint.GreaterThanConstraint;
import org.opendata.core.prune.MaxDropFinder;
import org.opendata.core.set.EntitySet;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.util.FormatedBigDecimal;
import org.opendata.curation.d4.signature.ContextSignatureGenerator;
import org.opendata.curation.d4.signature.SignatureValue;
import org.opendata.db.eq.EQ;
import org.opendata.db.eq.EQIndex;


/**
 * Print the terms in the robust signature of an equivalence class.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ContextSignatureBlockPrinter {

    public void run(
            File eqFile,
            File termFile,
            int nodeId,
            PrintWriter out
    ) throws java.io.IOException {
        
        EQIndex eqIndex = new EQIndex(eqFile);
        
        List<SignatureValue> sig = new ContextSignatureGenerator(eqIndex.nodes())
            .getSignature(nodeId)
            .rankedElements();
        
        HashIDSet termFilter = new HashIDSet();
        for (SignatureValue el : sig) {
            termFilter.add(eqIndex.get(el.id()).terms().first());
        }
        EntitySet terms = new EntitySet(termFile, termFilter);

        int blockCount = 0;

        int start = 0;
        final int end = sig.size();

        MaxDropFinder candidateFinder = new MaxDropFinder<>(
                new GreaterThanConstraint(BigDecimal.ZERO),
                false,
                true
        );

        while (start < end) {
            int dropIndex = candidateFinder.getPruneIndex(sig, start);
            if (dropIndex == 0) {
                break;
            }
            blockCount++;
            boolean isFirstNode = true;
            for (int iNode = start; iNode < dropIndex; iNode++) {
                String line = "";
                if (isFirstNode) {
                    line = "B" + blockCount;
                    line += " (" + (dropIndex - start) + ")";
                }
                SignatureValue el = sig.get(iNode);
                EQ node = eqIndex.get(el.id());
                int termId = node.terms().first();
                line += "\t" + terms.get(termId).name();
                line += "\t" + new FormatedBigDecimal(el.value(), 4);
                out.println(line);
                isFirstNode = false;
            }
            if (dropIndex < sig.size()) {
                FormatedBigDecimal delta = new FormatedBigDecimal(sig.get(dropIndex - 1).value() - sig.get(dropIndex).value());
                out.println("-- DROP " + delta);
            }
            start = dropIndex;
        }
    }

    private final static String COMMAND = 
            "Usage: \n" +
            "  <eq-file>\n" +
            "  <term-file>\n" +
            "  <eq-id>";
 
    public static void main(String[] args) {
	
        if (args.length != 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
	
        File eqFile = new File(args[0]);
        File termFile = new File(args[1]);
        int eqId = Integer.parseInt(args[2]);
	
        try (PrintWriter out = new PrintWriter(System.out)) {
            new ContextSignatureBlockPrinter()
                    .run(eqFile, termFile, eqId, out);
        } catch (java.io.IOException ex) {
            Logger.getGlobal().log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
