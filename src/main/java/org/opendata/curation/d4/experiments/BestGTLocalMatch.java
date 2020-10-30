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
import java.math.BigDecimal;
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
import org.opendata.core.util.FormatedBigDecimal;
import org.opendata.curation.d4.domain.Domain;
import org.opendata.curation.d4.domain.DomainReader;
import org.opendata.db.eq.EQIndex;

/**
 * Best match for ground truth domains against all local domains.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class BestGTLocalMatch {
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <eq-file>\n" +
            "  <gt-dir>\n" +
            "  <local-domain-file>";
    
    private static final Logger LOGGER = Logger.getLogger(BestGTLocalMatch.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        File gtDir = new File(args[1]);
        File localDomainFile = new File(args[2]);
        
        System.out.println("DOMAIN\tPRECISION\tRECALL\tF1");

        HashMap<String, IDSet> groundTruths = new HashMap<>();
        try {
            for (File file : gtDir.listFiles()) {
                String name = file.getName().substring(0, file.getName().indexOf("."));
                IDSet gt = new GTReader().read(file);
                groundTruths.put(name, gt);
            }
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }

        HashMap<String, BigDecimal[]> bestMatches = new HashMap<>();
        for (String key : groundTruths.keySet()) {
            BigDecimal[] matchInfo = new BigDecimal[]{
                BigDecimal.ZERO,
                BigDecimal.ZERO,
                BigDecimal.ZERO
            };
            bestMatches.put(key, matchInfo);
        }
        
        try {
            EQIndex eqIndex = new EQIndex(eqFile);
            for (Domain domain : new DomainReader(localDomainFile).read()) {
                HashIDSet terms = new HashIDSet();
                for (int nodeId : domain) {
                    terms.add(eqIndex.get(nodeId).terms());
                    if (terms.length() > 100000) {
                        break;
                    }
                }
                if (terms.length() > 100000) {
                    continue;
                }
                for (String key : groundTruths.keySet()) {
                    IDSet gt = groundTruths.get(key);
                    int ovp = terms.overlap(gt);
                    if (ovp > 0) {
                        Precision precision = new Precision(ovp, terms.length());
                        Recall recall = new Recall(ovp, gt.length());
                        BigDecimal f1 = new F1(precision, recall).value();
                        BigDecimal[] bestMatch = bestMatches.get(key);
                        if (bestMatch[2].compareTo(f1) < 0) {
                            bestMatch[0] = precision.value();
                            bestMatch[1] = recall.value();
                            bestMatch[2] = f1;
                        }
                    }
                }
            }
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }

        List<String> names = new ArrayList<>(groundTruths.keySet());
        Collections.sort(names);
        for (String name : names) {
            BigDecimal[] bestMatch = bestMatches.get(name);
            System.out.println(
                    String.format(
                            "%s\t%s\t%s\t%s",
                            name,
                            new FormatedBigDecimal(bestMatch[0]),
                            new FormatedBigDecimal(bestMatch[1]),
                            new FormatedBigDecimal(bestMatch[2])
                    )
            );
        }
    }
}
