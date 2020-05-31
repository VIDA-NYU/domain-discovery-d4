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
package org.opendata.core.sort;

import java.util.Comparator;

import org.opendata.core.object.IdentifiableDecimal;

/**
 * Sort identifiable decimals in descending order of the decimal value.
 * Use identifier (in ascending order) as the second sort key.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 *
 */
public class DecimalRanking implements Comparator<IdentifiableDecimal> {

	@Override
	public int compare(IdentifiableDecimal val0, IdentifiableDecimal val1) {

		int comp = val1.compareTo(val0);
		if (comp == 0) {
			comp = Integer.compare(val0.id(), val1.id());
		}
		return comp;
	}
}
