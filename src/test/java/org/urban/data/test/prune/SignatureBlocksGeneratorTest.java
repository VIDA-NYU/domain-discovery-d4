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
package org.urban.data.test.prune;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opendata.core.prune.MaxDropFinder;
import org.opendata.core.sort.DoubleValueDescSort;
import org.opendata.curation.d4.signature.SignatureBlock;
import org.opendata.curation.d4.signature.SignatureBlocksGenerator;
import org.opendata.curation.d4.signature.SignatureValue;

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
    private static List<SignatureValue> toList(double[] values) {
       
        ArrayList<SignatureValue> result = new ArrayList<>();
        
        for (double val : values) {
            result.add(new SignatureValue(result.size(), val));
        }
        
        Collections.sort(result, new DoubleValueDescSort<>());
        
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
        
        List<SignatureValue> values = toList(new double[]{0.8, 0.7, 0.5, 0.45});
        
        List<SignatureBlock> blocks;
        
        // Steepest drop finder that does not consider the full signature
        // constraint.
        MaxDropFinder dfNoConstraint = new MaxDropFinder<>(false, true);
        blocks = new SignatureBlocksGenerator(dfNoConstraint, false).toBlocks(values);
        
        // Splits values in three blocks [0.8-0.7], [0.5], [0.45].
        assertEquals(3, blocks.size());
        assertEquals(2, blocks.get(0).length());
        assertEquals(1, blocks.get(1).length());
        assertEquals(1, blocks.get(2).length());

        // Steepest drop finder that does  consider the full signature constraint.
        MaxDropFinder dfConstraint = new MaxDropFinder<>(true, true);
        blocks = new SignatureBlocksGenerator(dfConstraint, false).toBlocks(values);
        
        // Splits values into one block.
        assertEquals(1, blocks.size());
        assertEquals(4, blocks.get(0).length());
        
        // If the last drop is considered we always end up with a single block.
        for (boolean fullSig : new boolean[]{true, false}) {
            MaxDropFinder df = new MaxDropFinder<>(fullSig, false);
            blocks = new SignatureBlocksGenerator(df, false).toBlocks(values);
            assertEquals(1, blocks.size());
            assertEquals(4, blocks.get(0).length());
        }
    }
    
    @Test
    public void testMinorDropDonstraint() {
        
        List<SignatureValue> values = toList(new double[]{
            0.31,
            0.3,
            0.2,
            0.2,
            0.15,
            0.13,
            0.11,
            0.09,
            0.07,
            0.05,
            0.015,
            0.01,
            0.005,
            0.001
        });

        // The full signature constraint or the last drop should not have an
        // impact in this settting.
        for (boolean fullSig : new boolean[]{true, false}) {
            for (boolean ignoreLast : new boolean[]{true, false}) {
                MaxDropFinder df = new MaxDropFinder<>(fullSig, ignoreLast);
                List<SignatureBlock> blocks = new SignatureBlocksGenerator(df, true)
                        .toBlocks(values);
                assertEquals(3, blocks.size());
                assertEquals(2, blocks.get(0).length());
                assertEquals(2, blocks.get(1).length());
                assertEquals(10, blocks.get(2).length());
            }
        }
        
        // Same deltas but with steepest last drop.
        values = toList(new double[]{
            0.91,
            0.9,
            0.8,
            0.8,
            0.75,
            0.73,
            0.71,
            0.69,
            0.67,
            0.65,
            0.615,
            0.61,
            0.605,
            0.601
        });

        // In this case we need to ignore the last drop and the full signature
        // constraint.
        MaxDropFinder df = new MaxDropFinder<>(false, true);
        List<SignatureBlock> blocks = new SignatureBlocksGenerator(df, true)
                .toBlocks(values);
        assertEquals(3, blocks.size());
        assertEquals(2, blocks.get(0).length());
        assertEquals(2, blocks.get(1).length());
        assertEquals(10, blocks.get(2).length());
    }
}
