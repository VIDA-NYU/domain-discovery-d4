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
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.FileSystem;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.core.util.StringHelper;
import org.opendata.curation.d4.domain.Domain;
import org.opendata.curation.d4.domain.DomainReader;
import org.opendata.db.Database;
import org.opendata.db.eq.CompressedTermIndex;
import org.opendata.db.eq.CompressedTermIndexFile;
import org.opendata.db.eq.EQ;
import org.opendata.db.term.TermIndexReader;

/**
 * Best match for ground truth domains against all local domains.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class BestGTLocalMatchWriter {
    
    private void print(
            IDSet terms,
            String headline,
            HashMap<Integer, String> termIndex,
            boolean isFirst,
            PrintWriter out
    ) {
        if (!isFirst) {
            out.println();
        }
        out.println(headline);
        out.println(StringHelper.repeat("-", headline.length()));
        
        List<String> names = new ArrayList<>();
        for (int termId : terms) {
            names.add(termIndex.get(termId));
        }
        Collections.sort(names);
        for (String name : names) {
            out.println(name);
        }
    }
    
    public void run(
            CompressedTermIndex eqIndex,
            Database database,
            HashMap<String, IDSet> groundTruths,
            IdentifiableObjectSet<Domain> domains,
            File outputDir,
            int sizeThreshold
    ) throws java.io.IOException {
        
        HashMap<String, BestMatch> bestMatches;
        bestMatches = new BestGTLocalMatch(sizeThreshold)
                .run(eqIndex, groundTruths, domains);
        
        HashMap<Integer, Integer[]> termIndex = new HashMap<>();
        for (EQ eq : eqIndex) {
            termIndex.put(eq.id(), eq.terms());
        }
        
        HashIDSet termFilter = new HashIDSet();
        for (BestMatch match : bestMatches.values()) {
            for (int eqId : domains.get(match.domainId())) {
                for (int termId : termIndex.get(eqId)) {
                    termFilter.add(termId);
                }
            }
        }
        for (IDSet gt : groundTruths.values()) {
            termFilter.add(gt);
        }
        
        HashMap<Integer, String> terms = database.getTermIndex(termFilter);

        FileSystem.createFolder(outputDir);
        
        for (String name : bestMatches.keySet()) {
            BestMatch match = bestMatches.get(name);
            IDSet gt = groundTruths.get(name);
            HashIDSet domain = new HashIDSet();
            for (int eqId : domains.get(match.domainId())) {
                for (int termId : termIndex.get(eqId)) {
                    domain.add(termId);
                }
            }
            IDSet unmatched = gt.difference(domain);
            IDSet matched = gt.intersect(domain);
            IDSet extras = domain.difference(gt);
            File outFile = FileSystem.joinPath(outputDir, name + ".txt");
            try (PrintWriter out = FileSystem.openPrintWriter(outFile)) {
                this.print(matched, "MATCHED", terms, true, out);
                this.print(unmatched, "UNMATCHED", terms, false, out);
                this.print(extras, "EXTRA", terms, false, out);
            }
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

    private static final String COMMAND =
            "Usage:\n" +
            "  <eq-file>\n" +
            "  <term-file>\n" +
            "  <gt-dir>\n" +
            "  <local-domain-file>\n" +
            "  <output-dir>\n" +
            "  {<sizeThreshold> [default: 100000]}";
    
    private static final Logger LOGGER = Logger
            .getLogger(BestGTLocalMatchWriter.class.getName());
    
    public static void main(String[] args) {
        
        if ((args.length < 5) || (args.length > 6)) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        File termFile = new File(args[1]);
        File gtDir = new File(args[2]);
        File localDomainFile = new File(args[3]);
        File outputDir = new File(args[4]);
        
        int sizeThreshold = 100000;
        if (args.length == 6) {
            sizeThreshold = Integer.parseInt(args[5]);
        }
        
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

        try {
            CompressedTermIndex eqIndex = new CompressedTermIndexFile(eqFile);
            TermIndexReader termIndex = new TermIndexReader(termFile);
            new BestGTLocalMatchWriter().run(
                    eqIndex,
                    new Database(eqIndex, termIndex),
                    groundTruths,
                    new DomainReader(localDomainFile).read(),
                    outputDir,
                    sizeThreshold
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
