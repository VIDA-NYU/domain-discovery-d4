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
package org.opendata.curation.d4.signature.hierarchy;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.opendata.core.object.IdentifiableBigDecimal;
import org.opendata.core.sort.DecimalRanking;
import org.opendata.curation.d4.signature.ContextSignatureGenerator;
import org.opendata.curation.d4.signature.SignatureValue;

public class HierarchicalSignatureGenerator {

	private final ContextSignatureGenerator _signatures;
	
	public HierarchicalSignatureGenerator(ContextSignatureGenerator signatures) {
		
		_signatures = signatures;
	}
	
	public HierarchicalSignature getSignature(int nodeId) {
		
		List<SignatureValue> sig = _signatures
				.getSignature(nodeId)
				.rankedElements();
		
		List<IdentifiableBigDecimal> diffs = new ArrayList<>();
		int[] nodes = new int[sig.size()];
		for (int iRank = 1; iRank < sig.size(); iRank++) {
			SignatureValue node = sig.get(iRank - 1);
			SignatureValue next = sig.get(iRank);
			BigDecimal diff = node.value().subtract(next.value());
			if (diff.compareTo(BigDecimal.ZERO) > 0) {
				diffs.add(new IdentifiableBigDecimal(iRank, diff));
			}
			nodes[iRank - 1] = node.id();
		}
		// Add element for final drop.
		int lastIndex = sig.size() - 1;
		SignatureValue lastNode = sig.get(lastIndex);
		diffs.add(new IdentifiableBigDecimal(sig.size(), lastNode.value()));
		nodes[lastIndex] = lastNode.id();
		Collections.sort(diffs, new DecimalRanking());

		Bucket root = new SingleBucket(0, sig.size());
		int counter = 1;
		for (IdentifiableBigDecimal drop : diffs) {
			root = root.split(drop.id(), counter++);
		}
		
		return new HierarchicalSignature(root, nodes, diffs.size());
	}
}
