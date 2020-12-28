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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.FileSystem;
import org.opendata.core.set.StringSet;
import org.opendata.db.term.Term;
import org.opendata.db.term.TermConsumer;
import org.opendata.db.term.TermIndexReader;

/**
 * Helper method to prepare a list of ground-truth domain terms for evaluation.
 * Creates an output file that contains the term identifier and the term for all
 * terms from the input file that are present in the given term index.
 * 
 * @author @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class PrepareGTFile {
    
    private class GTTermWriter implements TermConsumer {

        private final PrintWriter _out;
        private final StringSet _terms;
        private int _writeCount;
        
        public GTTermWriter(StringSet terms, PrintWriter out) {
            
            _terms = terms;
            _out = out;
            
            _writeCount = 0;
        }
        
        @Override
        public void close() {

            System.out.println(
                    String.format(
                            "FOUND %d OF %d TERMS.",
                            _writeCount,
                            _terms.length()
                    )
            );
        }

        @Override
        public void consume(Term term) {

            if (_terms.contains(term.name())) {
                _out.println(String.format("%d\t%s", term.id(), term.name()));
                _writeCount++;
            }
        }

        @Override
        public void open() {
        
        }        
    }
    
    private final TermIndexReader _termIndex;
    
    public PrepareGTFile(TermIndexReader termIndex) {
        
        _termIndex = termIndex;
    }
    
    public void run(StringSet terms, PrintWriter out) {
        
        try {
            _termIndex.read(new GTTermWriter(terms, out));
        } catch (java.io.IOException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    private final static String COMMAND =
            "Usage:\n" +
            "  <term-file>\n" +
            "  <gt-file>\n" +
            "  <output-file>";
    
    private final static Logger LOGGER = Logger
            .getLogger(PrepareGTFile.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File termFile = new File(args[0]);
        File gtTermsFile = new File(args[1]);
        File outputFile = new File(args[2]);
        
        PrepareGTFile processor;
        processor = new PrepareGTFile(new TermIndexReader(termFile));
        
        if (gtTermsFile.isDirectory()) {
            for (File file : gtTermsFile.listFiles()) {
                File outFile = FileSystem.joinPath(outputFile, file.getName());
                System.out.println(outFile.getName());
                try (PrintWriter out = FileSystem.openPrintWriter(outFile)) {
                    processor.run(new StringSet(file), out);
                } catch (java.io.IOException ex) {
                    LOGGER.log(Level.SEVERE, "RUN", ex);
                    System.exit(-1);
                }
            }
        } else {
            try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
                processor.run(new StringSet(gtTermsFile), out);
            } catch (java.io.IOException ex) {
                LOGGER.log(Level.SEVERE, "RUN", ex);
                System.exit(-1);
            }
        }
    }
}
