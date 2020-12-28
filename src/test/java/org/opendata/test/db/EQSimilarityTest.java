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
package org.opendata.test.db;

import java.math.BigDecimal;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.opendata.core.metric.JaccardIndex;
import org.opendata.core.object.IdentifiableDouble;
import org.opendata.db.eq.similarity.JISimilarity;
import org.opendata.db.eq.similarity.LogJISimilarity;
import org.opendata.db.eq.similarity.SimilarityScore;
import org.opendata.db.eq.similarity.WeightedJISimilarity;

/**
 *
 * @author @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EQSimilarityTest {
    
    public EQSimilarityTest() {
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
    public void testJISimilarity() {
    
        JISimilarity simFunc = new JISimilarity(
                new Integer[][]{
                    {1, 2, 3, 4 ,5},
                    {1, 3, 5, 6},
                    {3, 5, 7},
                    {10, 11, 12}
                }
        );
        
        JaccardIndex ji = new JaccardIndex();
        
        SimilarityScore s01 = simFunc.sim(0, 1);
        assertEquals(3, simFunc.sim(0, 1).overlap());
        assertEquals(ji.sim(5, 4, 3), s01.score());
        
        SimilarityScore s02 = simFunc.sim(0, 2);
        assertEquals(2, s02.overlap());
        assertEquals(ji.sim(5, 3, 2), s02.score());
        
        SimilarityScore s12 = simFunc.sim(1, 2);
        assertEquals(2, s12.overlap());
        assertEquals(ji.sim(4, 3, 2), s12.score());
        
        SimilarityScore s03 = simFunc.sim(0, 3);
        assertEquals(0, s03.overlap());
        assertEquals(BigDecimal.ZERO, s03.score());
    }

    @Test
    public void testJILogSimilarity() {
    
        LogJISimilarity simFunc = new LogJISimilarity(
                new Integer[][] {
                    {1, 2, 3, 4 ,5},
                    {1, 3, 5, 6},
                    {3, 5, 7},
                    {10, 11, 12}
                }
        );
        
        JaccardIndex ji = new JaccardIndex();
        
        SimilarityScore s01 = simFunc.sim(0, 1);
        assertEquals(3, simFunc.sim(0, 1).overlap());
        assertEquals(ji.logSim(5, 4, 3), s01.score());
        
        SimilarityScore s02 = simFunc.sim(0, 2);
        assertEquals(2, s02.overlap());
        assertEquals(ji.logSim(5, 3, 2), s02.score());
        
        SimilarityScore s12 = simFunc.sim(1, 2);
        assertEquals(2, s12.overlap());
        assertEquals(ji.logSim(4, 3, 2), s12.score());
        
        SimilarityScore s03 = simFunc.sim(0, 3);
        assertEquals(0, s03.overlap());
        assertEquals(BigDecimal.ZERO, s03.score());
    }
    
    @Test
    public void testWeightedSimilarity() {
        
        WeightedJISimilarity simFunc = new WeightedJISimilarity(
                new IdentifiableDouble[][] {
                    {
                        new IdentifiableDouble(1, 0.1),
                        new IdentifiableDouble(2, 0.25),
                        new IdentifiableDouble(3, 0.5)
                    },
                    {
                        new IdentifiableDouble(2, 0.3),
                        new IdentifiableDouble(4, 0.05),
                        new IdentifiableDouble(5, 0.05)
                    }
                }
        );
        
        SimilarityScore s = simFunc.sim(0, 1);
        assertEquals(1, s.overlap());
        assertEquals(new BigDecimal(0.25 / (0.1 + 0.3 + 0.5 + 0.05 + 0.05)), s.score());
        
        s = simFunc.sim(1, 0);
        assertEquals(1, s.overlap());
        assertEquals(new BigDecimal(0.25 / (0.1 + 0.3 + 0.5 + 0.05 + 0.05)), s.score());
    }
}
