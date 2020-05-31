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
package org.opendata.curation.d4.signature.similarity;

import java.math.BigDecimal;

import org.opendata.core.metric.JaccardIndex;
import org.opendata.db.eq.Node;

/**
 * Compute node similarity as the Jaccard Similarity between the column
 * sets of two nodes using the logarithm to weight divisor and dividend.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 *
 */
public class ColumnSetLogJaccard implements NodeSimilarityFunction {

	private final JaccardIndex _ji = new JaccardIndex();

	@Override
	public BigDecimal eval(Node nodeI, Node nodeJ) {
		
		int overlap =  nodeI.overlap(nodeJ);
		if (overlap > 0) {
			return _ji.logSim(nodeI.columnCount(), nodeJ.columnCount(), overlap);
		} else {
			return BigDecimal.ZERO;
		}
	}
}
