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

import java.io.BufferedReader;
import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.opendata.core.io.FileSystem;
import org.opendata.core.profiling.datatype.DefaultDataTypeAnnotator;

/**
 * Print most frequent terms across different data sources.
 * 
 * @author heiko
 *
 */
public class TopKCrossSourceTerms {

	private class Term implements Comparable<Term> {
		
		private final int _columnCount;
		private final int _sourceCount;
		private final String _value;
		
		public Term(String value, String[] counts) {
			
			_value = value;
			_sourceCount = counts.length;
			
			int sum = 0;
			for (String c : counts) {
				sum += Integer.parseInt(c);
			}
			_columnCount = sum;
		}
		
		public int columnCount() {
			
			return _columnCount;
		}

		@Override
		public int compareTo(Term term) {
			
			int comp = Integer.compare(_sourceCount, term.sourceCount());
			if (comp == 0) {
				comp = Integer.compare(_columnCount, term.columnCount());
				if (comp == 0) {
					comp = _value.compareTo(term.value());
				}
			}
			return comp;
		}
		
		public int sourceCount() {
			
			return _sourceCount;
		}
		
		public String value() {
			
			return _value;
		}
	}
	
	public void run(File termFile, boolean textOnly, int k) throws java.io.IOException {
		
		int size = 0;
		Term[] ranking = new Term[k];
		
		DefaultDataTypeAnnotator typeAnnotator = new DefaultDataTypeAnnotator();
		
		try (BufferedReader in = FileSystem.openReader(termFile)) {
			String line;
			while ((line = in.readLine()) != null) {
				String[] tokens = line.split("\t");
				String value = tokens[0];
				if (textOnly) {
					if (!typeAnnotator.getType(value).isText()) {
						continue;
					}
				}
				Term term = new Term(tokens[0], tokens[1].split(","));
				if (size < ranking.length) {
					ranking[size++] = term;
					if (size == ranking.length) {
						Arrays.sort(ranking, Comparator.reverseOrder());
					}
				} else {
					if (ranking[size - 1].compareTo(term) < 0) {
						int index = 0;
						while ((index < size) && (ranking[index].compareTo(term) > 0)) {
							index++;
						}
						for (int iRank = size - 2; iRank >= index; iRank--) {
							ranking[iRank + 1] = ranking[iRank];
						}
						ranking[index] = term;
					}
				}
			}
		}
		
		for (int iRank = 0; iRank < size; iRank++) {
			Term term = ranking[iRank];
			System.out.println((iRank + 1) + "\t" + term.sourceCount() + "\t" + term.columnCount() + "\t" + term.value());
		}
	}
	
	private final static String COMMAND =
			"Usage:\n" +
			"  <term-file>\n" +
			"  <text-only>\n" +
			"  <k>";
	
	private final static Logger LOGGER = Logger
			.getLogger(TopKCrossSourceTerms.class.getName());
	
	public static void main(String[] args) {
		
		if (args.length != 3) {
			System.out.println(COMMAND);
			System.exit(-1);
		}
		
		File termFile = new File(args[0]);
		boolean textOnly = Boolean.parseBoolean(args[1]);
		int k = Integer.parseInt(args[2]);
		
		try (BufferedReader in = FileSystem.openReader(termFile)) {
			new TopKCrossSourceTerms().run(termFile, textOnly, k);
		} catch (java.io.IOException ex) {
			LOGGER.log(Level.SEVERE, "RUN", ex);
			System.exit(-1);
		}
	}
}
