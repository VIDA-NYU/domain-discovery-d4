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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.opendata.core.object.IdentifiableBigDecimal;
import org.opendata.core.sort.DecimalRanking;
import org.opendata.core.util.FormatedBigDecimal;
import org.opendata.curation.d4.signature.ContextSignatureGenerator;
import org.opendata.curation.d4.signature.SignatureValue;
import org.opendata.curation.d4.signature.similarity.ColumnSetJaccard;
import org.opendata.curation.d4.signature.similarity.ColumnSetLogJaccard;
import org.opendata.curation.d4.signature.similarity.NodeSimilarityFunction;
import org.opendata.db.eq.EQIndex;

public class HierarchicalContextSignaturePrinter {

	private final static String COMMAND =
			"Usage:\n" +
			"  <eq-file>\n" +
			"  <use-log> [true | false]\n" +
			"  <node-id>";
	
	private final static Logger LOGGER = Logger
			.getLogger(HierarchicalContextSignaturePrinter.class.getName());
	
	public static void main(String[] args) {
		
		if (args.length != 3) {
			System.out.println(COMMAND);
			System.exit(-1);
		}
		
		File eqFile = new File(args[0]);
		boolean useLog = Boolean.parseBoolean(args[1]);
		int nodeId = Integer.parseInt(args[2]);
		
		NodeSimilarityFunction simFunc;
		if (useLog) {
			simFunc = new ColumnSetJaccard();
		} else {
			simFunc = new ColumnSetLogJaccard();
		}
		
		EQIndex eqIndex = null;
		try {
			eqIndex = new EQIndex(eqFile);
		} catch (java.io.IOException ex) {
			LOGGER.log(Level.SEVERE, "RUN", ex);
			System.exit(-1);
		}

		ContextSignatureGenerator signatures = null;
		signatures = new ContextSignatureGenerator(
				eqIndex.nodes(),
				simFunc,
				true
		);

		List<SignatureValue> sig = signatures
				.getSignature(nodeId)
				.rankedElements();
		sig = new ArrayList<>(sig);
		sig.add(new SignatureValue(-1, BigDecimal.ZERO));
		
		List<IdentifiableBigDecimal> diffs = new ArrayList<>();
		for (int iRank = 0; iRank < sig.size() - 1; iRank++) {
			SignatureValue node = sig.get(iRank);
			SignatureValue next = sig.get(iRank + 1);
			BigDecimal diff = node.value().subtract(next.value());
			if (diff.compareTo(BigDecimal.ZERO) > 0) {
				diffs.add(new IdentifiableBigDecimal(iRank, diff));
			}
		}
		Collections.sort(diffs, new DecimalRanking());
		HashMap<Integer, Integer> drops = new HashMap<>();
		for (int iDrop = 0; iDrop < diffs.size(); iDrop++) {
			IdentifiableBigDecimal drop = diffs.get(iDrop);
			drops.put(drop.id(), iDrop);
		}
		
		for (int iRank = 0; iRank < sig.size() - 1; iRank++) {
			SignatureValue node = sig.get(iRank);
			SignatureValue next = sig.get(iRank + 1);
			BigDecimal diff = node.value().subtract(next.value());
			String drop = "";
			if (drops.containsKey(iRank)) {
				drop = Integer.toString(drops.get(iRank));
			}
			System.out.println(
					String.format(
							"%d\t%s\t%s\t%s",
							node.id(),
							node.toPlainString(),
							new FormatedBigDecimal(diff).toString(),
							drop
					)
			);
		}
	}
}
