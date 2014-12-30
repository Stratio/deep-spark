/*
 * Copyright 2014, Stratio.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.stratio.deep.core.extractor;

import static com.stratio.deep.commons.utils.CellsUtils.getObjectFromJson;
import static org.testng.Assert.assertEquals;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.json.simple.JSONObject;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.stratio.deep.commons.config.BaseConfig;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.exception.DeepTransformException;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.core.entity.BookEntity;
import com.stratio.deep.core.entity.CantoEntity;
import com.stratio.deep.core.entity.PlayerEntity;
import com.stratio.deep.core.entity.TeamEntity;
import com.stratio.deep.core.entity.WordCount;

import scala.Tuple2;

/**
 * Created by rcrespo on 17/11/14.
 */
public abstract class ExtractorEntityTest<T, S extends BaseConfig<T>> extends ExtractorTest<T, S> {

    private static final long serialVersionUID = 6367238996895716537L;

    /**
     * @param extractor
     * @param host
     * @param port
     * @param isCells
     */
    public ExtractorEntityTest(Class extractor, String host, Integer port, boolean isCells, Class<T> dataSetClass) {
        super(extractor, host, port, isCells, dataSetClass);
    }

    public ExtractorEntityTest(Class extractor, String host, Integer port, boolean isCells) {
        super(extractor, host, port, isCells);
    }

    @Override
    public Object transform(JSONObject jsonObject, String nameSpace, Class entityClass) {
        try {
            return getObjectFromJson(entityClass, jsonObject);
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new DeepTransformException(e.getMessage());
        }
    }

    /**
     * Prepares team data to use in join tests. Maps teams to pair with id as key.
     *
     * @param context the deep context
     */
    protected JavaPairRDD<Long, TeamEntity> prepareTeamRDD(DeepSparkContext context) {
        ExtractorConfig<TeamEntity> teamsConfigEntity = getReadExtractorConfig(this.databaseExtractorName,
                FOOTBALL_TEAM_INPUT, TeamEntity.class);

        return context.createJavaRDD(teamsConfigEntity)
                .mapToPair
                        (new PairFunction<TeamEntity, Long, TeamEntity>() {
                            @Override
                            public Tuple2<Long, TeamEntity> call(TeamEntity teamEntity)
                                    throws Exception {
                                return new Tuple2<>(teamEntity.getId(), teamEntity);
                            }
                        });
    }

    /**
     * Prepares player data to use in join tests. Maps players to pair with teamId as key.
     *
     * @param context the deep context
     */
    protected JavaPairRDD<Long, PlayerEntity> preparePlayerRDD(DeepSparkContext context) {
        ExtractorConfig<PlayerEntity> playersConfigEntity = getReadExtractorConfig(this.databaseExtractorName,
                FOOTBALL_PLAYER_INPUT, PlayerEntity.class);

        return context.createJavaRDD(playersConfigEntity)
                .mapToPair
                        (new PairFunction<PlayerEntity, Long, PlayerEntity>() {
                            @Override
                            public Tuple2<Long, PlayerEntity> call(PlayerEntity playerEntity)
                                    throws Exception {
                                return new Tuple2<>(playerEntity.getTeamId(), playerEntity);
                            }
                        });
    }

    /**
     * It tests if the extractor can group by key players data set
     */
    @Test
    protected void testGroupByKey() {
        DeepSparkContext context = getDeepSparkContext();

        try {

            JavaPairRDD<Long, PlayerEntity> playersRDD = preparePlayerRDD(context);

            assertEquals(playersRDD.count(), 5);
            assertEquals(playersRDD.groupByKey().count(), 4);

        } finally {
            context.stop();
        }
    }

    /**
     * It tests if the extractor can filter players data set
     */
    @Test
    protected void testFilterSpark() {
        DeepSparkContext context = getDeepSparkContext();

        try {

            JavaPairRDD<Long, PlayerEntity> playersRDD = preparePlayerRDD(context);

            JavaPairRDD<Long, PlayerEntity> playersFilteredEqRDD = playersRDD.filter(
                    new Function<Tuple2<Long, PlayerEntity>, Boolean>() {
                        @Override
                        public Boolean call(Tuple2<Long, PlayerEntity> player) throws Exception {
                            return player._2().getId() == 3;
                        }
                    });

            JavaPairRDD<Long, PlayerEntity> playersFilteredNEqRDD = playersRDD.filter(
                    new Function<Tuple2<Long, PlayerEntity>, Boolean>() {
                        @Override
                        public Boolean call(Tuple2<Long, PlayerEntity> player) throws Exception {
                            return player._2().getId() != 1;
                        }
                    });

            assertEquals(playersRDD.count(), 5);
            assertEquals(playersFilteredEqRDD.count(), 1);
            assertEquals(playersFilteredEqRDD.collect().size(), 1);
            assertEquals(playersFilteredEqRDD.first()._2().getId().longValue(), 3L);
            assertEquals(playersFilteredNEqRDD.count(), 4);
            assertEquals(playersFilteredNEqRDD.collect().size(), 4);

        } finally {
            context.stop();
        }
    }

    /**
     * It tests if the extractor can join two data sets
     */
    @Test
    protected void testInnerJoin() {
        DeepSparkContext context = getDeepSparkContext();

        try {

            JavaPairRDD<Long, TeamEntity> teamsRDD = prepareTeamRDD(context);

            JavaPairRDD<Long, Iterable<PlayerEntity>> playersRDD = preparePlayerRDD(context).groupByKey();

            JavaPairRDD<Long, Tuple2<TeamEntity, Iterable<PlayerEntity>>> joinRDD = teamsRDD.join(playersRDD);

            assertEquals(joinRDD.count(), 4);

        } finally {
            context.stop();
        }
    }

    /**
     * It tests if the extractor can left join two data sets
     */
    @Test
    protected void testLeftOuterJoin() {
        DeepSparkContext context = getDeepSparkContext();

        try {

            JavaPairRDD<Long, TeamEntity> teamsRDD = prepareTeamRDD(context);

            JavaPairRDD<Long, Iterable<PlayerEntity>> playersRDD = preparePlayerRDD(context).groupByKey();

            JavaPairRDD<Long, Tuple2<TeamEntity, Optional<Iterable<PlayerEntity>>>> joinRDD =
                    teamsRDD.leftOuterJoin(playersRDD);

            assertEquals(joinRDD.count(), teamsRDD.count());

        } finally {
            context.stop();
        }
    }

    /**
     * It tests if the extractor can right join two data sets
     */
    @Test
    protected void testRightOuterJoin() {
        DeepSparkContext context = getDeepSparkContext();

        try {

            JavaPairRDD<Long, TeamEntity> teamsRDD = prepareTeamRDD(context);

            JavaPairRDD<Long, PlayerEntity> playersRDD = preparePlayerRDD(context);

            JavaPairRDD<Long, Tuple2<Optional<TeamEntity>, PlayerEntity>> joinRDD =
                    teamsRDD.rightOuterJoin(playersRDD);

            assertEquals(joinRDD.count(), playersRDD.count());

        } finally {
            context.stop();
        }
    }

    @Test(alwaysRun = true)
    public void testDataSet() {

        DeepSparkContext context = getDeepSparkContext();
        try {

            ExtractorConfig<BookEntity> inputConfigEntity = getReadExtractorConfig(databaseExtractorName, BOOK_INPUT,
                    BookEntity.class);

            RDD<BookEntity> inputRDDEntity = context.createRDD(inputConfigEntity);

            //Import dataSet was OK and we could read it
            assertEquals(1, inputRDDEntity.count());

            List<BookEntity> books = inputRDDEntity.toJavaRDD().collect();

            BookEntity book = books.get(0);

            //      tests subDocuments
            assertEquals(((BookEntity) originBook).getMetadataEntity().getAuthor(),
                    book.getMetadataEntity().getAuthor());

            //      tests List<subDocuments>
            List<CantoEntity> listCantos = ((BookEntity) originBook).getCantoEntities();

            for (int i = 0; i < listCantos.size(); i++) {
                assertEquals(listCantos.get(i).getNumber(), book.getCantoEntities().get(i).getNumber());
                assertEquals(listCantos.get(i).getText(), book.getCantoEntities().get(i).getText());
            }

            RDD<BookEntity> inputRDDEntity2 = context.createRDD(inputConfigEntity);

            JavaRDD<String> words = inputRDDEntity2.toJavaRDD().flatMap(new FlatMapFunction<BookEntity, String>() {
                @Override
                public Iterable<String> call(BookEntity bookEntity) throws Exception {

                    List<String> words = new ArrayList<>();
                    for (CantoEntity canto : bookEntity.getCantoEntities()) {
                        words.addAll(Arrays.asList(canto.getText().split(" ")));
                    }
                    return words;
                }
            });

            JavaPairRDD<String, Long> wordCount = words.mapToPair(new PairFunction<String, String, Long>() {
                @Override
                public Tuple2<String, Long> call(String s) throws Exception {
                    return new Tuple2<String, Long>(s, 1l);
                }
            });

            JavaPairRDD<String, Long> wordCountReduced = wordCount.reduceByKey(new Function2<Long, Long, Long>() {
                @Override
                public Long call(Long integer, Long integer2) throws Exception {
                    return integer + integer2;
                }
            });

            JavaRDD<WordCount> outputRDD = wordCountReduced.map(new Function<Tuple2<String, Long>, WordCount>() {
                @Override
                public WordCount call(Tuple2<String, Long> stringIntegerTuple2) throws Exception {
                    return new WordCount(stringIntegerTuple2._1(), stringIntegerTuple2._2());
                }
            });

            ExtractorConfig<WordCount> outputConfigEntity = getWriteExtractorConfig(BOOK_OUTPUT, WordCount.class);

            context.saveRDD(outputRDD.rdd(), outputConfigEntity);

            RDD<WordCount> outputRDDEntity = context.createRDD(outputConfigEntity);

            Assert.assertEquals(WORD_COUNT_SPECTED.longValue(), ((Long) outputRDDEntity.cache().count()).longValue());

        } finally {
            context.stop();
        }

    }
}
