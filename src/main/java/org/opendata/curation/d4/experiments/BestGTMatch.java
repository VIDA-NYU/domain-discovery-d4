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
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.metric.F1;
import org.opendata.core.metric.Precision;
import org.opendata.core.metric.Recall;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.MutableIdentifiableIDSet;
import org.opendata.core.util.FormatedBigDecimal;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class BestGTMatch {
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <gt-dir>\n" +
            "  <domain-dir>\n" +
            "  <first-block-only> [true | false]";
    
    private static final Logger LOGGER = Logger.getLogger(BestGTMatch.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File gtDir = new File(args[0]);
        File domainDir = new File(args[1]);
        boolean firstBlockOnly = Boolean.parseBoolean(args[2]);
        
        System.out.println("DOMAIN\tID\tPRECISION\tRECALL\tF1");

        try {
            List<MutableIdentifiableIDSet> domains;
            domains = new StrongDomainJsonReader().readAll(domainDir, firstBlockOnly);
            for (File file : gtDir.listFiles()) {
                String name = file.getName().substring(0, file.getName().indexOf("."));
                IDSet gt = new GTReader().read(file);
                BigDecimal[] bestMatch = new BigDecimal[]{
                    BigDecimal.ZERO,
                    BigDecimal.ZERO,
                    BigDecimal.ZERO
                };
                int bestMatchID = -1;
                for (MutableIdentifiableIDSet domain : domains) {
                    int ovp = domain.overlap(gt);
                    if (ovp > 0) {
                        Precision precision = new Precision(ovp, domain.length());
                        Recall recall = new Recall(ovp, gt.length());
                        BigDecimal f1 = new F1(precision, recall).value();
                        if (bestMatch[2].compareTo(f1) < 0) {
                            bestMatch = new BigDecimal[]{
                                precision.value(),
                                recall.value(),
                                f1
                            };
                            bestMatchID = domain.id();
                        }
                    }
                }
                System.out.println(
                        String.format(
                                "%s\t%d\t%s\t%s\t%s",
                                name,
                                bestMatchID,
                                new FormatedBigDecimal(bestMatch[0]),
                                new FormatedBigDecimal(bestMatch[1]),
                                new FormatedBigDecimal(bestMatch[2])
                        )
                );
            }
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}