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
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.EntitySetReader;
import org.opendata.core.io.FileSystem;
import org.opendata.core.set.EntitySet;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.util.StringHelper;
import org.opendata.db.eq.EQIndex;

/**
 * Print information from EQ summary report files. Expects an input file with
 * five columns:
 * 
 * 1) EQ identifier
 * 2) Number of columns the EQ occurs in
 * 3) Number of columns the EQ was added to be expansion
 * 4) Number of local domains the EQ occurs in
 * 5) Number of strong domains the EQ occurs in
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EQPrinter {
 
    public void print(
            EQIndex nodeIndex,
            EntitySetReader termReader,
            List<EQSummary> nodes,
            PrintWriter out
    ) throws java.io.IOException {
        
        // Read terms (max. 10 terms per equivalence class).
        HashIDSet termFilter = new HashIDSet();
        for (EQSummary node : nodes) {
            int[] termIds = nodeIndex.get(node.id()).terms().toArray();
            for (int iTerm = 0; iTerm < Math.min(termIds.length, 10); iTerm++) {
                termFilter.add(termIds[iTerm]);
            }
        }
        
        EntitySet terms = termReader.readEntities(termFilter);
        
        for (EQSummary node : nodes) {
            List<String> names = new ArrayList<>();
            int[] termIds = nodeIndex.get(node.id()).terms().toArray();
            for (int iTerm = 0; iTerm < Math.min(termIds.length, 10); iTerm++) {
                names.add(terms.get(termIds[iTerm]).name());
            }
            Collections.sort(names);
            out.println(
                    names.get(0) + "\t" +
                    StringHelper.joinStrings(node.properties(), "\t")
            );
            for (int iTerm = 1; iTerm < names.size(); iTerm++) {
                out.println(names.get(iTerm));
            }
        }
    }
    
    private final static String COMMAND =
            "Usage:\n" +
            "  <eq-file>\n" +
            "  <term-file>\n" +
            "  <eq-summary-file>\n" +
            "  <output-file>";
    
    private final static Logger LOGGER = Logger
            .getLogger(EQPrinter.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 4) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        File termFile = new File(args[1]);
        File summaryFile = new File(args[2]);
        File outputFile = new File(args[3]);
        
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            new EQPrinter()
                    .print(
                            new EQIndex(eqFile),
                            new EntitySetReader(termFile),
                            new EQSummaryReader(summaryFile).read(),
                            out
                    );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
