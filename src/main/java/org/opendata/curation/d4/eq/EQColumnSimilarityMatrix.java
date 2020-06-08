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
package org.opendata.curation.d4.eq;

import org.opendata.db.Database;
import org.opendata.db.column.Column;
import org.opendata.db.eq.EQIndex;

public class EQColumnSimilarityMatrix {

	private final Database _db;
	private final EQIndex _eqIndex;
	
	public EQColumnSimilarityMatrix(EQIndex eqIndex) {
		
		_eqIndex = eqIndex;
		_db = new Database(_eqIndex);
	}
	
	public double getSim(Column colI, Column colJ) {
		
		double sim;
		if (colI.id() == colJ.id()) {
			sim = 1;
		} else {
			int overlap = colI.overlap(colJ);
			if (overlap > 1) {
				sim = (double)(overlap - 1)/(double)((colI.length() + colJ.length()) - (overlap + 1));
			} else {
				sim = 0;
			}
		}
		
		return sim;
	}
	
	public double getSim(int colI, int colJ) {
		
		return this.getSim(_db.columns().get(colI), _db.columns().get(colJ));
	}
}
