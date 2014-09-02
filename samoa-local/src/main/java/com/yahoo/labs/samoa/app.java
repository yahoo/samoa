/**
 * Created with IntelliJ IDEA.
 * User: amir
 * Date: 2014-08-26
 * Time: 16:20
 */
package com.yahoo.labs.samoa;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2013 - 2014 Yahoo! Inc.
 * %%
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
 * #L%
 */

import com.yahoo.labs.samoa.sentinel.model.TwitterStreamInstance;
import com.yahoo.labs.samoa.tasks.PrequentialEvaluation;
import com.yahoo.labs.samoa.topology.impl.SimpleComponentFactory;

public class app
{
    public static void main( String[] args ) {
        PrequentialEvaluation pe = new PrequentialEvaluation();
        pe.setFactory(new SimpleComponentFactory());

        pe.dumpFileOption.setValueViaCLIString("/tmp/dump.csv");
        pe.instanceLimitOption.setValue(50);
        pe.sampleFrequencyOption.setValue(5);
        pe.learnerOption.setValueViaCLIString("classifiers.trees.VerticalHoeffdingTree -p 1");
        pe.streamTrainOption.setValueViaCLIString(TwitterStreamInstance.class.getName());

        pe.init();
    }
}
