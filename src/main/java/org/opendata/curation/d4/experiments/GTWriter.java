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

import java.io.BufferedReader;
import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.FileSystem;
import org.opendata.db.eq.EQ;
import org.opendata.db.eq.EQIndex;
import org.opendata.db.term.Term;
import org.opendata.db.term.TermConsumer;
import org.opendata.db.term.TermIndexReader;

/**
 * Find node and term identifier for ground truth domain terms.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class GTWriter {
    
    private class TermCollector implements TermConsumer {

        private final HashSet<String> _names;
        private HashMap<String, Term> _terms;
        
        public TermCollector(HashSet<String> names) {
            
            _names = names;
        }
        
        @Override
        public void close() {

        }

        @Override
        public void consume(Term term) {

            if (_names.contains(term.name())) {
                _terms.put(term.name(), term);
            }
        }

        @Override
        public void open() {

            _terms = new HashMap<>();
        }
        
        public HashMap<String, Term> terms() {
            
            return _terms;
        }
    }
    
    public void run(
            EQIndex eqIndex,
            TermIndexReader termReader,
            HashSet<String> terms,
            PrintWriter out
    ) throws java.io.IOException {

        TermCollector collector = new TermCollector(terms);
        termReader.read(collector);
        
        System.out.println(
                String.format(
                        "FOUND %d OF %d TERMS",
                        collector.terms().size(),
                        terms.size()
                )
        );
        
        List<String> foundTerms = new ArrayList<>(collector.terms().keySet());
        Collections.sort(foundTerms);
        
        for (String name : foundTerms) {
            Term term = collector.terms().get(name);
            for (EQ node : eqIndex) {
                if (node.terms().contains(term.id())) {
                    out.println(String.format("%d\t%d\t%s", node.id(), term.id(), name));
                    break;
                }
            }
        }
    }
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <eq-index>\n" +
            "  <term-index>\n" +
            "  <input-dir>\n" +
            "  <output-dir>";
    
    public static void main(String[] args) {
        
        if (args.length != 4) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        File termIndex = new File(args[1]);
        File inputDir = new File(args[2]);
        File outputDir = new File(args[3]);

        FileSystem.createFolder(outputDir);
        
        try {
            EQIndex eqIndex = new EQIndex(eqFile);
            TermIndexReader termReader = new TermIndexReader(termIndex);
            for (File inFile : inputDir.listFiles()) {
                System.out.println(inFile.getName());
                File outFile = FileSystem.joinPath(outputDir, inFile.getName());
                HashSet<String> terms = new HashSet<>();
                try (BufferedReader in = FileSystem.openReader(inFile)) {
                    String line;
                    while ((line = in.readLine()) != null) {
                        terms.add(line.split("\t")[2]);
                    }
                }
                try (PrintWriter out = FileSystem.openPrintWriter(outFile)) {
                    new GTWriter().run(eqIndex, termReader, terms, out);
                }
            }
        } catch (java.io.IOException ex) {
            Logger.getLogger(GTWriter.class.getName()).log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
