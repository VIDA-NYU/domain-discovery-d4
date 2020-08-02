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

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IDSet;
import org.opendata.curation.d4.Constants;
import org.opendata.db.Database;

/**
 * Create a tokenized version of a given tem index. Tokenizes each term
 * using the default tokenizer. Creates a new term index for the generated
 * tokens.
 * 
 * @author Heiko Mueller
 *
 */
public class TermIndexTokenizer implements TermConsumer {

	private final IDSet _columnFilter;
	private final TermIndexGenerator _termIndex;
	private final DefaultTokenizer _tokenizer;
	
	public TermIndexTokenizer(
			File outputFile,
			int bufferSize,
			IDSet columnFilter
	) throws java.io.IOException {
		
		_termIndex = new TermIndexGenerator(outputFile, bufferSize);
		_tokenizer = new DefaultTokenizer();
		_columnFilter = columnFilter;
	}
	
	@Override
	public void close() {

		_termIndex.close();
	}

	@Override
	public void consume(Term term) {

		HashIDSet columns = (HashIDSet)term.columns().intersect(_columnFilter);
		if (columns.isEmpty()) {
			return;
		}
		for (String token : _tokenizer.tokens(term.name())) {
			_termIndex.consume(new Term(token, columns));
		}
	}

	@Override
	public void open() {

		_termIndex.open();
	}

    
    private final static String COMMAND =
	    "Usage:\n" +
	    "  <term-index-file>\n" +
		"  <eq-file>\n" +
	    "  <mem-buffer-size>\n" +
	    "  <output-file>";
    
    private final static Logger LOGGER = Logger
    		.getLogger(TermIndexTokenizer.class.getName());
    
    public static void main(String[] args) {
        
        System.out.println("Term Index Tokenizer - Version (" + Constants.VERSION + ")");

        if (args.length != 4) {
            System.out.println(COMMAND);
            System.exit(-1);
        }

        File inputFile = new File(args[0]);
        File eqFile = new File(args[1]);
        int bufferSize = Integer.parseInt(args[2]);
        File outputFile = new File(args[3]);
        
        try {
        	new TermIndexReader(inputFile).read(
        			new TermIndexTokenizer(
    						outputFile,
    						bufferSize,
    						new Database(eqFile).columnIds()
    				)
        	);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "CREATE TERM INDEX", ex);
            System.exit(-1);
        }
    }
}
