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
package org.opendata.db.term;

import org.opendata.core.set.StringSet;

/**
 * The default term tokenizer (1) replaces '-', '_', '/' with a
 * single whitespace, (2) removes all special characters (non-digit,
 * non-word, and non-whitespace), and (3) splits the string on
 * whitespace characters.
 * 
 * @author Heiko Mueller
 *
 */
public class DefaultTokenizer implements TermTokenizer {

	@Override
	public StringSet tokens(String text) {
 
		String[] tokens = text
				.replaceAll("[-_/]", " ")
				.replaceAll("[^\\w\\d\\s]", "")
				.split("\\s+");
		return new StringSet(tokens);
	}
}
