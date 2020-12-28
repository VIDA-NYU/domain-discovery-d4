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
package org.opendata.curation.d4.evaluate;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.set.IDSet;
import org.opendata.curation.d4.domain.DomainReader;
import org.opendata.db.eq.CompressedTermIndexFile;

/**
 * Best match for ground truth domains against all local domains.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class BestGTLocalMatchPrinter {
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <eq-file>\n" +
            "  <gt-dir>\n" +
            "  <local-domain-file>\n" +
            "  {<sizeThreshold> [default: 100000]}";
    
    private static final Logger LOGGER = Logger
            .getLogger(BestGTLocalMatchPrinter.class.getName());
    
    public static void main(String[] args) {
        
        if ((args.length < 3) || (args.length > 4)) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        File gtDir = new File(args[1]);
        File localDomainFile = new File(args[2]);
        
        int sizeThreshold = 100000;
        if (args.length == 4) {
            sizeThreshold = Integer.parseInt(args[3]);
        }
        
        System.out.println("GT\tID\tPRECISION\tRECALL\tF1");

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

        HashMap<String, BestMatch> bestMatches = null;
        try {
            bestMatches = new BestGTLocalMatch(sizeThreshold).run(
                    new CompressedTermIndexFile(eqFile),
                    groundTruths,
                    new DomainReader(localDomainFile).read()
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
        
        if (bestMatches != null) {
            List<String> names = new ArrayList<>(groundTruths.keySet());
            Collections.sort(names);
            for (String name : names) {
                BestMatch match = bestMatches.get(name);
                System.out.println(
                        String.format(
                                "%s\t%d\t%s\t%s\t%s",
                                name,
                                match.domainId(),
                                match.precision().toString(),
                                match.recall().toString(),
                                match.f1().toString()
                        )
                );
            }
        }
    }
}
