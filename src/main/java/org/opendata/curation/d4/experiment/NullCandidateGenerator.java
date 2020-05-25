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
package org.opendata.curation.d4.experiment;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.opendata.core.io.FileSystem;
import org.opendata.curation.d4.NoExpandDomainGenerator;
import org.opendata.curation.profiling.NullValueCandidateWriter;

public class NullCandidateGenerator {
	
	private final boolean _overwrite;
	
	public NullCandidateGenerator(boolean overwrite) {
		
		_overwrite = overwrite;
	}
	
	public void run(
			Datasource ds,
			boolean strict,
			int threads
	) throws java.lang.InterruptedException, java.io.IOException {
		
		NoExpandDomainGenerator domainGenerator;
		domainGenerator = new NoExpandDomainGenerator(ds.directory(), _overwrite);
		domainGenerator.run(ds.eqFile(),  threads);
		String filename;
		if (strict) {
			filename = "null-candidates.strict.tsv";
		} else {
			filename = "null-candidates.weak.tsv";
		}
		File outputFile = FileSystem.joinPath(ds.directory(),filename);
		new NullValueCandidateWriter(strict).run(
				ds.eqFile(),
				ds.termFile(),
				domainGenerator.signatureFile(),
				domainGenerator.columnFile(),
				domainGenerator.localDomainFile(),
				domainGenerator.strongDomainFile(),
				outputFile
		);
	}
	
    private static final String COMMAND =
            "Usage:\n" +
            "  <domain-dir>\n" +
    		"  <overwrite>\n" +
            "  <strict>\n" +
            "  <threads>";
    
    private static final Logger LOGGER = Logger
    		.getLogger(NullCandidateGenerator.class.getName());
    
	public static void main(String[] args) {
		
		if (args.length != 4) {
			System.out.println(COMMAND);
			System.exit(-1);
		}
		
		File baseDir = new File(args[0]);
		boolean overwrite = Boolean.parseBoolean(args[1]);
		boolean strict = Boolean.parseBoolean(args[2]);
		int threads = Integer.parseInt(args[3]);
		
		try {
			new NullCandidateGenerator(overwrite)
			.run(new Datasource(baseDir), strict, threads);
		} catch (java.lang.InterruptedException | java.io.IOException ex) {
			LOGGER.log(Level.SEVERE, "RUN", ex);
			System.exit(-1);
		}
	}
}
