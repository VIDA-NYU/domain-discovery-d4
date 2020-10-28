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
import java.math.RoundingMode;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.metric.F1;
import org.opendata.core.metric.Precision;
import org.opendata.core.metric.Recall;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.MutableIdentifiableIDSet;

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
        
        try {
            List<MutableIdentifiableIDSet> domains;
            domains = new StrongDomainJsonReader().readAll(domainDir, firstBlockOnly);
            for (File file : gtDir.listFiles()) {
                String name = file.getName().substring(0, file.getName().indexOf("."));
                IDSet gt = new GTReader().read(file);
                BigDecimal maxF1 = BigDecimal.ZERO;
                for (MutableIdentifiableIDSet domain : domains) {
                    int ovp = domain.overlap(gt);
                    if (ovp > 0) {
                        BigDecimal f1 = new F1(
                                new Precision(ovp, domain.length()),
                                new Recall(ovp, gt.length())
                        ).value();
                        if (maxF1.compareTo(f1) < 0) {
                            maxF1 = f1;
                        }
                    }
                }
                System.out.println(String.format("%s\t%s", name, maxF1.setScale(6, RoundingMode.HALF_DOWN).toPlainString()));
            }
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
