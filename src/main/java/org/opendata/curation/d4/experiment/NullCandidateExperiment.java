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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.opendata.core.io.FileSystem;

public class NullCandidateExperiment {

	public void run(
			File baseDir,
			int sizeThreshold,
			boolean overwrite,
			boolean strict,
			int threads
	) throws java.lang.InterruptedException, java.io.IOException {
		
		// Read available data sources (domains)
		List<Datasource> datasources = new ArrayList<>();
		for (File dir : baseDir.listFiles()) {
			if (dir.isDirectory()) {
				File eqFile = FileSystem.joinPath(dir, "compressed-term-index.TEXT.txt.gz");
				File termFile = FileSystem.joinPath(dir, "term-index.txt.gz");
				if ((eqFile.exists()) && (termFile.exists())) {
					datasources.add(new Datasource(dir, eqFile, termFile));
				} else {
					System.out.println("SKIP " + dir.getName());
				}
			}
		}
		Collections.sort(datasources);
		
		for (Datasource ds : datasources) {
			if (ds.size() < sizeThreshold) {
				System.out.println("PROCESS " + ds.name() + " OF SIZE " + ds.size());
				new NullCandidateGenerator(overwrite).run(ds, strict, threads);
			} else {
				System.out.println("SKIP " + ds.name() + " OF SIZE " + ds.size());
			}
		}
	}
	
    private static final String COMMAND =
            "Usage:\n" +
            "  <base-dir>\n" +
    		"  <size-threshold>\n" +
    		"  <overwrite>\n" +
            "  <strict>\n" +
            "  <threads>";
    
    private static final Logger LOGGER = Logger
    		.getLogger(NullCandidateExperiment.class.getName());
    
	public static void main(String[] args) {
		
		if (args.length != 5) {
			System.out.println(COMMAND);
			System.exit(-1);
		}
		
		File baseDir = new File(args[0]);
		int sizeThreshold = Integer.parseInt(args[1]);
		boolean overwrite = Boolean.parseBoolean(args[2]);
		boolean strict = Boolean.parseBoolean(args[3]);
		int threads = Integer.parseInt(args[4]);
		
		try {
			new NullCandidateExperiment()
				.run(baseDir, sizeThreshold, overwrite, strict, threads);
		} catch (java.lang.InterruptedException | java.io.IOException ex) {
			LOGGER.log(Level.SEVERE, "RUN", ex);
			System.exit(-1);
		}
	}
}
