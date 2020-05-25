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
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.opendata.core.io.EntitySetReader;
import org.opendata.core.object.IdentifiableBigDecimal;
import org.opendata.core.set.EntitySet;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.sort.BigDecimalAscSort;
import org.opendata.core.util.FormatedBigDecimal;
import org.opendata.curation.d4.signature.SignatureBlocks;
import org.opendata.curation.d4.signature.SignatureBlocksConsumer;
import org.opendata.curation.d4.signature.SignatureBlocksReader;
import org.opendata.curation.d4.signature.SignatureBlocksStream;
import org.opendata.db.eq.EQ;
import org.opendata.db.eq.EQIndex;

/**
 * Print top-k terms based on low maximum somilarity values in their
 * context signatures.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class LowSimSignatureTermPrinter {

	private class Score {
		
		private final EQ _node;
		private final FormatedBigDecimal _weight;
		
		public Score(EQ node, IdentifiableBigDecimal weight) {
			
			_node = node;
			_weight = weight.toFormatedDecimal();
		}
		
		public EQ node() {
			
			return _node;
		}
		
		public FormatedBigDecimal weight() {
			
			return _weight;
		}
	}
	
	private interface ScoreFunction {
		
		public IdentifiableBigDecimal eval(IdentifiableBigDecimal node);
	}
	
	private class MaxSimScore implements ScoreFunction {

		@Override
		public IdentifiableBigDecimal eval(IdentifiableBigDecimal node) {
			return node;
		}
	}
	
	private class NormalizedMaxSimScore implements ScoreFunction {

		private final EQIndex _eqIndex;
		
		public NormalizedMaxSimScore(EQIndex eqIndex) {
			
			_eqIndex = eqIndex;
		}
		
		@Override
		public IdentifiableBigDecimal eval(IdentifiableBigDecimal node) {
			BigDecimal columns;
			columns = new BigDecimal(_eqIndex.get(node).columns().length());
			BigDecimal weight;
			weight = node.value().divide(columns, MathContext.DECIMAL64);
			return new IdentifiableBigDecimal(node.id(), weight);
		}
	}
	
	private class SignatureStatsCollector implements SignatureBlocksConsumer {

		private final EQIndex _eqIndex;
		private final List<IdentifiableBigDecimal> _signatures;
		
		public SignatureStatsCollector(EQIndex eqIndex) {
			
			_eqIndex = eqIndex;
			_signatures = new ArrayList<>();
		}
		
		@Override
		public void close() {
			
		}

		@Override
		public void consume(SignatureBlocks sig) {

			_signatures.add(new IdentifiableBigDecimal(sig.id(), sig.maxSim()));
		}
		
		public List<Score> getTopK(int k, ScoreFunction func) {
			
			ArrayList<IdentifiableBigDecimal> weights = new ArrayList<>();
			for (IdentifiableBigDecimal sig : _signatures) {
				weights.add(func.eval(sig));
			}
			Collections.sort(weights, new BigDecimalAscSort<>());
			
			List<Score> result = new ArrayList<>();
			for (IdentifiableBigDecimal weight : weights) {
				result.add(new Score(_eqIndex.get(weight), weight));
				if (result.size() == k) {
					break;
				}
			}
			return result;
		}

		@Override
		public void open() {
			
		}
	}
	
	public void run(
			EQIndex eqIndex,
			EntitySetReader termReader,
			SignatureBlocksStream signatures,
			boolean columnWeights,
			int k
	) throws java.io.IOException {
		
		SignatureStatsCollector stats = new SignatureStatsCollector(eqIndex);
		signatures.stream(stats);
		
		ScoreFunction func;
		if (columnWeights) {
			func = new NormalizedMaxSimScore(eqIndex);
		} else {
			func = new MaxSimScore();
		}
		
		List<Score> nodes = stats.getTopK(k, func);
		
		HashIDSet filter = new HashIDSet();
		for (Score score : nodes) {
			filter.add(score.node().id());
		}
		EntitySet terms = termReader.readEntities(eqIndex, filter);
		
		for (Score el : nodes) {
			for (int termId : el.node().terms()) {
				System.out.println(
						terms.get(termId).name() + "\t" +
						el.node().columns().length() + "\t" +
						el.weight()
				);
			}
		}
	}
	
    private static final String COMMAND =
            "Usage:\n" +
            "  <eq-file>\n" +
            "  <term-file>\n" +
            "  <signature-blocks-file>\n" +
            "  <column-weigths>\n" +
            "  <k>";
    
    private static final Logger LOGGER = Logger
            .getLogger(LowSimSignatureTermPrinter.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 5) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        File termFile = new File(args[1]);
        File signatureFile = new File(args[2]);
        boolean columnWeights = Boolean.parseBoolean(args[3]);
        int k = Integer.parseInt(args[4]);
        
        try  {
            new LowSimSignatureTermPrinter()
            	.run(
            			new EQIndex(eqFile),
            			new EntitySetReader(termFile),
            			new SignatureBlocksReader(signatureFile),
            			columnWeights,
            			k
            	);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
