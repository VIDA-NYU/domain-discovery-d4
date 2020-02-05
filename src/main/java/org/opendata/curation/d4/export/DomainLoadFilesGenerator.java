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
import org.opendata.core.set.HashIDSet;

/**
 * Convert a database domains file into load files for relational database.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DomainLoadFilesGenerator {
   
    private static final String COMMAND = 
	    "Usage:\n" +
	    "  <strong-domain-file>\n" +
	    "  <local-domain-file>\n" +
            "  <eq-file>\n" +
            "  <term-load-file>\n" +
            "  <prefix>\n" +
	    "  <output-directory>";
    
    public static void main(String[] args) {
	
	System.out.println(Constants.NAME + " - Domain Load File Generator - Version (" + Constants.VERSION + ")\n");

	if (args.length != 6) {
	    System.out.println(COMMAND);
	    System.exit(-1);
	}
	
	File strongDomainFile = new File(args[0]);
	File localDomainFile = new File(args[1]);
	File eqFile = new File(args[2]);
        File termLoadFile = new File(args[3]);
        String prefix = args[4];
	File outputDir = new File(args[5]);
	
        File scriptFile = FileSystem.joinPath(outputDir, "load-domains.sql");
        
        FileSystem.createParentFolder(scriptFile);
        
        try (PrintWriter script = FileSystem.openPrintWriter(scriptFile)) {
            new DatabaseLoadFileWriter().writeDatabaseDomainFile(strongDomainFile,
                    localDomainFile,
                    eqFile,
                    prefix + "domain",
                    prefix + "domain_column_map",
                    prefix + "domain_term_map",
                    prefix + "annotations",
                    new HashIDSet(termLoadFile),
                    FileSystem.joinPath(outputDir, "domain.del"),
                    FileSystem.joinPath(outputDir, "domain_column_map.del"),
                    FileSystem.joinPath(outputDir, "domain_term_map.del"),
                    script
            );
	} catch (java.io.IOException ex) {
	    Logger.getGlobal().log(Level.SEVERE, "RUN", ex);
	    System.exit(-1);
	}
    }
}
