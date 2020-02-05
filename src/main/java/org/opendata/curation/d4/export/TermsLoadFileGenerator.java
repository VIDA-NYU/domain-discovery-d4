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
package org.opendata.curation.d4.export;

import java.io.File;
import java.io.PrintWriter;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.curation.d4.Constants;
import org.opendata.core.io.FileSystem;

/**
 * Write load file for the terms table. Contains the unique term identifier
 * and term value.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TermsLoadFileGenerator {
    
    public void run(
            File termIndexFile,
            File eqFile,
            int threshold,
            int maxTermLength,
            String prefix,
            File outputDirectory
    ) throws java.io.IOException {

        File scriptFile = FileSystem.joinPath(outputDirectory, "load-terms.sql");
        File termFile = FileSystem.joinPath(outputDirectory, "term.del");
        File columnTermMapFile = FileSystem.joinPath(outputDirectory, "column_term_map.del");
        //File termNodeMapFile = FileSystem.joinPath(outputDirectory, "term_node_map.del");
        //File columnNodeMapFile = FileSystem.joinPath(outputDirectory, "column_node_map.del");
        
        FileSystem.createParentFolder(scriptFile);

        try (PrintWriter script = FileSystem.openPrintWriter(scriptFile)) {
            new DatabaseLoadFileWriter().writeTermsAndEquivalenceClasses(
                    eqFile,
                    termIndexFile,
                    prefix + "term",
                    prefix + "column_term_map",
                    prefix + "columns",
                    //prefix + "term_node_map",
                    //prefix + "column_node_map",
                    threshold,
                    maxTermLength,
                    termFile,
                    columnTermMapFile,
                    //termNodeMapFile,
                    //columnNodeMapFile,
                    script
            );
        }
    }
   
    private static final String COMMAND =
            "Usage:\n" +
            "  <term-index-file>\n" +
            "  <eq-file>\n" +
            "  <eq-term-threshold>\n" +
            "  <value-length-threshold>\n" +
            "  <prefix>\n" +
            "  <output-directory>";
    
    public static void main(String[] args) {
        
	System.out.println(Constants.NAME + " - Terms Load File Generator - Version (" + Constants.VERSION + ")\n");

        if (args.length != 6) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File termFile = new File(args[0]);
        File eqFile = new File(args[1]);
        int threshold = Integer.parseInt(args[2]);
        int maxTermLength = Integer.parseInt(args[3]);
        String prefix = args[4];
        File outputDirectory = new File(args[5]);
        
        try {
            new TermsLoadFileGenerator().run(
                    termFile,
                    eqFile,
                    threshold,
                    maxTermLength,
                    prefix,
                    outputDirectory
            );
        } catch (java.io.IOException ex) {
            Logger.getGlobal().log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
