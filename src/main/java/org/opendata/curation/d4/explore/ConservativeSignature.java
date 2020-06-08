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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.curation.d4.signature.SignatureBlocksConsumer;
import org.opendata.curation.d4.signature.SignatureBlocksReader;
import org.opendata.core.set.IDSet;
import org.opendata.core.util.StringHelper;
import org.opendata.curation.d4.signature.SignatureBlocks;
import org.opendata.curation.d4.signature.SignatureBlocksStream;
import org.opendata.db.Database;
import org.opendata.db.column.Column;
import org.opendata.db.eq.EQIndex;

/**
 * Generator for local domains using undirected graphs. Each connected component
 * in the graph generated from the robust signatures of the column elements 
 * represents a local domain.
 * 
 * Uses a concurrent queue to distribute columns across workers. Relies
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ConservativeSignature {
                   
	private class ConservativeSignaturePrinter implements SignatureBlocksConsumer {

		private final IDSet _nodes;
		
		public ConservativeSignaturePrinter(IDSet nodes) {
			
			_nodes = nodes;
		}
		
		@Override
		public void close() {
			
		}

		@Override
		public void consume(SignatureBlocks sig) {
			
			if (_nodes.contains(sig.id())) {
				String firstBlock;
				if (!sig.isEmpty()) {
					firstBlock = StringHelper.joinIntegers(sig.get(0));
				} else {
					firstBlock = "";
				}
				System.out.println(sig.id() + "\t" + firstBlock);
			}
		}

		@Override
		public void open() {
			
		}
	}
	
    public void run(
            EQIndex eqIndex,
            SignatureBlocksStream signatures,
            int columnId
    ) throws java.io.IOException {

    	Column column;
    	column = new Database(eqIndex).columns().get(columnId);
        
    	ConservativeSignaturePrinter printer;
    	printer = new ConservativeSignaturePrinter(column);
    	signatures.stream(printer);
    }
    
    private static final String COMMAND =
            "Usage\n" +
            "  <eq-file>\n" +
            "  <signature-file(s)>\n" +
            "  <column-id>";
    
    private static final Logger LOGGER = Logger
            .getLogger(ConservativeSignature.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        File signatureFile = new File(args[1]);
        int columnId = Integer.parseInt(args[2]);
        
        try {
            // Read the node index and the list of columns
            EQIndex nodeIndex = new EQIndex(eqFile);
            new ConservativeSignature().run(
                    nodeIndex,
                    new SignatureBlocksReader(signatureFile),
                    columnId
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
