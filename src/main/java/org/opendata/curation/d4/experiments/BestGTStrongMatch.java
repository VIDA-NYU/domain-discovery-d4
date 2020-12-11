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
package org.opendata.curation.d4.experiments;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.metric.F1;
import org.opendata.core.metric.Precision;
import org.opendata.core.metric.Recall;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.curation.d4.domain.StrongDomain;
import org.opendata.curation.d4.domain.StrongDomainReader;
import org.opendata.db.eq.EQIndex;

/**
 * Find best match of discovered strong domains with ground-truth domains over
 * all strong domain block sub-sequences.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class BestGTStrongMatch {
    
    private void match(
            int domainId,
            IDSet terms,
            IDSet gt,
            BestMatch[] matches,
            List<Integer> context
    ) {
        
        int ovp = terms.overlap(gt);
        if (ovp > 0) {
            Precision precision = new Precision(ovp, terms.length());
            Recall recall = new Recall(ovp, gt.length());
            F1 f1 = new F1(precision, recall);
            for (int iMatch : context) {
                if (matches[iMatch].f1().compareTo(f1) < 0) {
                    matches[iMatch] = new BestMatch(domainId, precision, recall);
                }
            }
        }
    }
    
    public void run(
            EQIndex eqIndex,
            IdentifiableObjectSet<StrongDomain> strongDomains,
            HashMap<String, IDSet> groundTruths
    ) {
        HashMap<String, BestMatch[]> bestMatches = new HashMap<>();
        for (String name : groundTruths.keySet()) {
            bestMatches.put(
                    name,
                    new BestMatch[]{new BestMatch(), new BestMatch(), new BestMatch()}
            );
        }
        
        for (StrongDomain domain : strongDomains) {
            List<IDSet> blocks;
            blocks = domain.getNodeBlocks(eqIndex);
            HashIDSet terms = new HashIDSet();
            for (int iBlock = 0; iBlock < blocks.size(); iBlock++) {
                List<Integer> context = new ArrayList<>();
                if (iBlock == 0) {
                    context.add(0);
                }
                context.add(1);
                if (iBlock == blocks.size() - 1) {
                    context.add(2);
                }
                IDSet block = blocks.get(iBlock);
                if ((terms.length() + block.length()) > 10000) {
                    break;
                }
                int blockSize = 0;
                for (int nodeId : block) {
                    blockSize += eqIndex.get(nodeId).terms().length();
                }
                if ((terms.length() + blockSize) > 10000) {
                    break;
                }
                for (int nodeId : block) {
                    terms.add(eqIndex.get(nodeId).terms());
                }
                for (String name : groundTruths.keySet()) {
                    IDSet gt = groundTruths.get(name);
                    this.match(domain.id(), terms, gt, bestMatches.get(name), context);
                }
            }
        }
        
        System.out.print("DOMAIN");
        for (String key : new String[]{"1", "?", "n"}) {
            System.out.print(
                    String.format(
                            "\tID (%s)\tPRECISION (%s)\tRECALL (%s)\tF1 (%s)",
                            key,
                            key,
                            key,
                            key
                    )
            );
        }
        System.out.println();
        
        List<String> names = new ArrayList<>(bestMatches.keySet());
        Collections.sort(names);
        
        for (String name : names) {
            System.out.print(name);
            for (BestMatch match : bestMatches.get(name)) {
                System.out.print(
                        String.format(
                                "\t%d\t%s\t%s\t%s",
                                match.domainId(),
                                match.precision().toString(),
                                match.recall().toString(),
                                match.f1().toString()
                        )
                );
            }
            System.out.println();
        }
    }

    private static final String COMMAND =
            "Usage:\n" +
            "  <eq-file>\n" +
            "  <gt-dir>\n" +
            "  <strong-domains-file>";
    
    private static final Logger LOGGER = Logger
            .getLogger(BestGTStrongMatch.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        File gtDir = new File(args[1]);
        File strongDomainFile = new File(args[2]);
        
        // Read ground truth domains.
        HashMap<String, IDSet> groundTruths = new HashMap<>();
        try {
            for (File file : gtDir.listFiles()) {
                String name = file.getName().substring(0, file.getName().indexOf("."));
                groundTruths.put(name, new GTReader().read(file));
            }
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "READ GT", ex);
            System.exit(-1);
        }
        
        try {
            new BestGTStrongMatch()
                    .run(
                            new EQIndex(eqFile),
                            new StrongDomainReader(strongDomainFile).read(),
                            groundTruths
                    );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
