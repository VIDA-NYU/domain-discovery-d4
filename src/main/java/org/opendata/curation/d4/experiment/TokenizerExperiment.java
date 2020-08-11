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

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.opendata.core.constraint.Threshold;
import org.opendata.core.io.FileSystem;
import org.opendata.core.metric.JaccardIndex;
import org.opendata.core.metric.Recall;
import org.opendata.core.util.StringHelper;
import org.opendata.db.eq.EQIndex;
import org.opendata.db.tools.SocrataHelper;

public class TokenizerExperiment {

	public void run(
			int[] colSizeText,
			int[] colSizeTokens,
			HashMap<Integer, String[]> columns,
			HashMap<String, Integer> ovpText,
			File ovpFileTokens,
			Threshold threshold,
			PrintWriter out
	) throws java.io.IOException {
		try (BufferedReader in = FileSystem.openReader(ovpFileTokens)) {
			String line;
			while ((line = in.readLine()) != null) {
				String[] tokens = line.split("\t");
				int colI = Integer.parseInt(tokens[0]);
				int colJ = Integer.parseInt(tokens[1]);
				int sizeTxtI = colSizeText[colI];
				int sizeTxtJ = colSizeText[colJ];
				int overlapTxt = 0;
				String key = String.format("%d#%d", colI, colJ);
				if (ovpText.containsKey(key)) {
					overlapTxt = ovpText.get(key);
				}
				int sizeTokensI = colSizeTokens[colI];
				int sizeTokensJ = colSizeTokens[colJ];
				int overlapTokens = Integer.parseInt(tokens[2]);
				BigDecimal jiDiff = new JaccardIndex()
						.sim(sizeTokensI, sizeTokensJ, overlapTokens)
						.subtract(new JaccardIndex().sim(sizeTxtI, sizeTxtJ, overlapTxt));
				if (threshold.isSatisfied(jiDiff)) {
					String sI = String.format("%s (%d)", StringHelper.joinStrings(columns.get(colI), "."), colI);
					String sJ = String.format("%s (%d)", StringHelper.joinStrings(columns.get(colJ), "."), colJ);
					out.println(
							String.format(
									"%s\t%s\t%s",
									sI,
									sJ,
									jiDiff.setScale(4, RoundingMode.HALF_DOWN).toPlainString()
							)
					);
				}
			}
		}
	}
	
	private final static String COMMAND =
			"Usage:\n" +
			"  <eq-original>\n" +
			"  <eq-tokens>\n" +
			"  <columns-file>\n" +
			"  <overlap-original>\n" +
			"  <overlap-tokens>\n" +
			"  <diff-threshold>\n" +
			"  <output-file>";
	
	private final static Logger LOGGER = Logger
			.getLogger(TokenizerExperiment.class.getName());
	
	public static void main(String[] args) {
		
		if (args.length != 7) {
			System.out.println(COMMAND);
			System.exit(-1);
		}
		
		File eqFileText = new File(args[0]);
		File eqFileTokens = new File(args[1]);
		File columnsFile = new File(args[2]);
		File ovpFileText = new File(args[3]);
		File ovpFileTokens = new File(args[4]);
		Threshold threshold = Threshold.getConstraint(args[5]);
		File outputFile = new File(args[6]);
		
		// Read column name and dataset identifier
		HashMap<Integer, String[]> columns = new HashMap<>();
		try {
			CSVParser parser = new CSVParser(
	                new InputStreamReader(FileSystem.openFile(columnsFile)),
	                CSVFormat.TDF
	        );
			for (CSVRecord row : parser) {
				int columnId = Integer.parseInt(row.get(0));
				String dataset = row.get(1);
				String columnName = row.get(2).replaceAll("\\s+", " ").trim();
				columns.put(columnId, new String[] {dataset, columnName});
			}
		} catch (java.io.IOException ex) {
			LOGGER.log(Level.SEVERE, "READ COLUMNS", ex);
			System.exit(-1);
		}
		
		HashMap<String, Integer> ovpText = new HashMap<>();
		try (BufferedReader in = FileSystem.openReader(ovpFileText)) {
			String line;
			while ((line = in.readLine()) != null) {
				String[] tokens = line.split("\t");
				String key = String.format("%s#%s", tokens[0], tokens[1]);
				ovpText.put(key,  Integer.parseInt(tokens[2]));
			}
		} catch (java.io.IOException ex) {
			LOGGER.log(Level.SEVERE, "READ", ex);
			System.exit(-1);
		}
		
		try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
			new TokenizerExperiment().run(
					new EQIndex(eqFileText).columnSizes(),
					new EQIndex(eqFileTokens).columnSizes(),
					columns,
					ovpText,
					ovpFileTokens,
					threshold,
					out
			);
		} catch (java.io.IOException ex) {
			LOGGER.log(Level.SEVERE, "RUN", ex);
			System.exit(-1);
		}
	}
}
