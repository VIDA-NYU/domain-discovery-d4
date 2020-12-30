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
package org.opendata.test.core.prune;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opendata.core.prune.MaxDropFinder;
import org.opendata.curation.d4.signature.ContextSignature;
import org.opendata.curation.d4.signature.ContextSignatureBlock;
import org.opendata.curation.d4.signature.ContextSignatureBlocksIndex;
import org.opendata.curation.d4.signature.SignatureBlock;
import org.opendata.curation.d4.signature.ContextSignatureProcessor;
import org.opendata.curation.d4.signature.ContextSignatureValue;

/**
 * Unit tests for the signature blocks generator.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SignatureBlocksGeneratorTest {
    
    /**
     * Helper method to generate a sorted list of signature values.
     * 
     * @param values
     * @return 
     */
    private static List<SignatureBlock> toBlocks(
            Integer[] eqTermCounts,
            MaxDropFinder<ContextSignatureValue> dropFinder,
            boolean ignoreMinorDrop,
            BigDecimal[] values
    ) {
       
        ContextSignatureProcessor processor;
        processor = new ContextSignatureProcessor(
                eqTermCounts,
                dropFinder,
                ignoreMinorDrop,
                false
        );
        
        List<ContextSignatureValue> elements = new ArrayList<>();
        for (BigDecimal value : values) {
            elements.add(new ContextSignatureValue(elements.size(), 0, value));
        }
        ContextSignature sig = new ContextSignature(0, elements);
        
        ContextSignatureBlocksIndex buf = new ContextSignatureBlocksIndex();
        buf.open();
        processor.process(sig, buf);
        buf.close();
        
        ArrayList<SignatureBlock> result = new ArrayList<>();
        
        for (ContextSignatureBlock block : buf.get(0)) {
            result.add(block);
        }
        
        return result;
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    @Test
    public void testFullSignatureConstraint() {
        
        BigDecimal[] values = new BigDecimal[] {
            new BigDecimal(0.8),
            new BigDecimal(0.7),
            new BigDecimal(0.5),
            new BigDecimal(0.45)
        };
        
        Integer[] eqTermCounts = new Integer[]{1, 1, 1, 1};
        
        List<SignatureBlock> blocks;
        
        // Steepest drop finder that does not consider the full signature
        // constraint.
        MaxDropFinder dfNoConstraint = new MaxDropFinder<>(false, true);
        blocks = toBlocks(eqTermCounts, dfNoConstraint, false, values);
        
        // Splits values in three blocks [0.8-0.7], [0.5], [0.45].
        assertEquals(3, blocks.size());
        assertEquals(2, blocks.get(0).elementCount());
        assertEquals(1, blocks.get(1).elementCount());
        assertEquals(1, blocks.get(2).elementCount());

        // Steepest drop finder that does  consider the full signature constraint.
        MaxDropFinder dfConstraint = new MaxDropFinder<>(true, true);
        blocks = toBlocks(eqTermCounts, dfConstraint, false, values);
        
        // Splits values into one block.
        assertEquals(1, blocks.size());
        assertEquals(4, blocks.get(0).elementCount());
        
        // If the last drop is considered we always end up with a single block.
        for (boolean fullSig : new boolean[]{true, false}) {
            MaxDropFinder df = new MaxDropFinder<>(fullSig, false);
            blocks = toBlocks(eqTermCounts, df, false, values);
            assertEquals(1, blocks.size());
            assertEquals(4, blocks.get(0).elementCount());
        }
    }
    
    @Test
    public void testMinorDropDonstraint() {
        
        BigDecimal[] values = new BigDecimal[] {
            new BigDecimal(0.31),
            new BigDecimal(0.3),
            new BigDecimal(0.2),
            new BigDecimal(0.2),
            new BigDecimal(0.15),
            new BigDecimal(0.13),
            new BigDecimal(0.11),
            new BigDecimal(0.09),
            new BigDecimal(0.07),
            new BigDecimal(0.05),
            new BigDecimal(0.015),
            new BigDecimal(0.01),
            new BigDecimal(0.005),
            new BigDecimal(0.001)
        };

        Integer[] eqTermCounts = new Integer[values.length];
        for (int i = 0; i < eqTermCounts.length; i++) {
            eqTermCounts[i] = 1;
        }

        // The full signature constraint or the last drop should not have an
        // impact in this settting.
        for (boolean fullSig : new boolean[]{true, false}) {
            for (boolean ignoreLast : new boolean[]{true, false}) {
                MaxDropFinder df = new MaxDropFinder<>(fullSig, ignoreLast);
                List<SignatureBlock> blocks;
                blocks = toBlocks(eqTermCounts, df, true, values);
                assertEquals(3, blocks.size());
                assertEquals(2, blocks.get(0).elementCount());
                assertEquals(2, blocks.get(1).elementCount());
                assertEquals(10, blocks.get(2).elementCount());
            }
        }
        
        // Same deltas but with steepest last drop.
        values = new BigDecimal[] {
            new BigDecimal(0.91),
            new BigDecimal(0.9),
            new BigDecimal(0.8),
            new BigDecimal(0.8),
            new BigDecimal(0.75),
            new BigDecimal(0.73),
            new BigDecimal(0.71),
            new BigDecimal(0.69),
            new BigDecimal(0.67),
            new BigDecimal(0.65),
            new BigDecimal(0.615),
            new BigDecimal(0.61),
            new BigDecimal(0.605),
            new BigDecimal(0.601)
        };
        
        eqTermCounts = new Integer[values.length];
        for (int i = 0; i < eqTermCounts.length; i++) {
            eqTermCounts[i] = 1;
        }

        // In this case we need to ignore the last drop and the full signature
        // constraint.
        MaxDropFinder df = new MaxDropFinder<>(false, true);
        List<SignatureBlock> blocks = toBlocks(eqTermCounts, df, true, values);
        assertEquals(3, blocks.size());
        assertEquals(2, blocks.get(0).elementCount());
        assertEquals(2, blocks.get(1).elementCount());
        assertEquals(10, blocks.get(2).elementCount());
    }
}
