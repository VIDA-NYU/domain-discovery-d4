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
package org.opendata.curation.d4.signature.trim;

import java.util.List;
import org.opendata.core.set.IDSet;
import org.opendata.curation.d4.signature.RobustSignatureConsumer;
import org.opendata.curation.d4.signature.SignatureBlock;
import org.opendata.curation.d4.signature.SingleBlockSignature;

/**
 * Conservative signature blocks trimmer. The conservative trimmer prunes all
 * but the first block of the given signature.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ConservativeTrimmer extends SignatureTrimmer {

    public ConservativeTrimmer(
            IDSet column,
            RobustSignatureConsumer consumer
    ) {
        super(column, consumer);
    }

    @Override
    public void trim(int id, List<SignatureBlock> blocks, RobustSignatureConsumer consumer) {

        consumer.consume(new SingleBlockSignature(id, blocks.get(0)));
    }
}
