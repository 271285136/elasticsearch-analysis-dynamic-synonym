package com.bellszhu.elasticsearch.plugin.synonym.analysis;


import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.util.fst.FST;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.AnalysisMode;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;

/**
 * @author bellszhu
 */
public class DynamicSynonymTokenFilterFactory extends
        AbstractTokenFilterFactory {

    private static final Logger logger = LogManager.getLogger("dynamic-synonym");

    /**
     * Static id generator
     */
    private static final AtomicInteger id = new AtomicInteger(1);
    private static final ScheduledExecutorService pool = Executors.newScheduledThreadPool(1, r -> {
        Thread thread = new Thread(r);
        thread.setName("monitor-synonym-Thread-" + id.getAndAdd(1));
        return thread;
    });
    private static WeakHashMap<IndexSettings, DynamicSynonymTokenFilterFactory> factoryWeakHashMap = new WeakHashMap();
    private volatile ScheduledFuture<?> scheduledFuture;

    private final String location;
    private final boolean expand;
    private final boolean lenient;
    private final String format;
    private final int interval;
    protected SynonymMap synonymMap;
    protected Map<AbsSynonymFilter, Integer> dynamicSynonymFilters = new WeakHashMap<>();
    protected final Environment environment;
    protected final AnalysisMode analysisMode;

    public DynamicSynonymTokenFilterFactory(
            IndexSettings indexSettings,
            Environment env,
            String name,
            Settings settings
    ) throws IOException {
        super(indexSettings, name, settings);
        logger.info("=======DynamicSynonymTokenFilterFactory========");
        factoryWeakHashMap.put(indexSettings, this);
        this.location = settings.get("synonyms_path");
        if (this.location == null) {
            throw new IllegalArgumentException(
                    "dynamic synonym requires `synonyms_path` to be configured");
        }
        if (settings.get("ignore_case") != null) {
        }

        this.interval = settings.getAsInt("interval", 60);
        this.expand = settings.getAsBoolean("expand", true);
        this.lenient = settings.getAsBoolean("lenient", false);
        this.format = settings.get("format", "");
        boolean updateable = settings.getAsBoolean("updateable", false);
        this.analysisMode = updateable ? AnalysisMode.SEARCH_TIME : AnalysisMode.ALL;
        this.environment = env;
    }

    @Override
    public AnalysisMode getAnalysisMode() {
        return this.analysisMode;
    }


    @Override
    public TokenStream create(TokenStream tokenStream) {
        throw new IllegalStateException(
                "Call getChainAwareTokenFilterFactory to specialize this factory for an analysis chain first");
    }

    public TokenFilterFactory getChainAwareTokenFilterFactory(
            TokenizerFactory tokenizer,
            List<CharFilterFactory> charFilters,
            List<TokenFilterFactory> previousTokenFilters,
            Function<String, TokenFilterFactory> allFilters
    ) {
        final Analyzer analyzer = buildSynonymAnalyzer(tokenizer, charFilters, previousTokenFilters, allFilters);
        synonymMap = buildSynonyms(analyzer);
        final String name = name();
        return new TokenFilterFactory() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                // fst is null means no synonyms
                if (synonymMap.fst == null) {
                    return tokenStream;
                }
                DynamicSynonymFilter dynamicSynonymFilter = new DynamicSynonymFilter(tokenStream, synonymMap, false);
                dynamicSynonymFilters.put(dynamicSynonymFilter, 1);

                return dynamicSynonymFilter;
            }

            @Override
            public TokenFilterFactory getSynonymFilter() {
                // In order to allow chained synonym filters, we return IDENTITY here to
                // ensure that synonyms don't get applied to the synonym map itself,
                // which doesn't support stacked input tokens
                return IDENTITY_FILTER;
            }

            @Override
            public AnalysisMode getAnalysisMode() {
                return analysisMode;
            }
        };
    }

    Analyzer buildSynonymAnalyzer(
            TokenizerFactory tokenizer,
            List<CharFilterFactory> charFilters,
            List<TokenFilterFactory> tokenFilters,
            Function<String, TokenFilterFactory> allFilters
    ) {
        return new CustomAnalyzer(
                tokenizer,
                charFilters.toArray(new CharFilterFactory[0]),
                tokenFilters.stream().map(TokenFilterFactory::getSynonymFilter).toArray(TokenFilterFactory[]::new)
        );
    }

    SynonymMap buildSynonyms(Analyzer analyzer) {
        try {
            return getSynonymFile(analyzer).reloadSynonymMap();
        } catch (Exception e) {
            logger.error("failed to build synonyms", e);
            throw new IllegalArgumentException("failed to build synonyms", e);
        }
    }

    SynonymFile getSynonymFile(Analyzer analyzer) {
        logger.info("=====getSynonymFile=======");
        try {
            SynonymFile synonymFile;
            if (location.startsWith("http://") || location.startsWith("https://")) {
                synonymFile = new RemoteSynonymFile(
                        environment, analyzer, expand, lenient,  format, location);
            } else {
                synonymFile = new LocalSynonymFile(
                        environment, analyzer, expand, lenient, format, location);
            }
            if (scheduledFuture == null) {
                scheduledFuture = pool.scheduleAtFixedRate(new Monitor(synonymFile),
                                interval, interval, TimeUnit.SECONDS);
            }
            return synonymFile;
        } catch (Exception e) {
            logger.error("failed to get synonyms: " + location, e);
            throw new IllegalArgumentException("failed to get synonyms : " + location, e);
        }
    }

    public void closeSchedule() {
        logger.info("=====closeSchedule=======");
        if (scheduledFuture != null && !scheduledFuture.isCancelled()) {
            logger.info("=====closeSchedule=======cancel");
            scheduledFuture.cancel(true);
        }
        scheduledFuture = null;
    }

    public class Monitor implements Runnable {

        private SynonymFile synonymFile;

        Monitor(SynonymFile synonymFile) {
            this.synonymFile = synonymFile;
        }

        @Override
        public void run() {
            if (scheduledFuture == null || scheduledFuture.isCancelled()) {
                return;
            }
            try {
                logger.info("===== Monitor =======");
                if (synonymFile.isNeedReloadSynonymMap()) {
                    synonymMap = synonymFile.reloadSynonymMap();
                    for (AbsSynonymFilter dynamicSynonymFilter : dynamicSynonymFilters.keySet()) {
                        dynamicSynonymFilter.update(synonymMap);
                        logger.debug("success reload synonym");
                    }
                }
            } catch (Exception e) {
                logger.info("Monitor error", e);
//                e.printStackTrace();
                logger.error(e);
            }
        }
    }

    public static void afterIndexShardDeleted() {}

    public static void afterIndexShardClosed(IndexSettings settings) {
        Set<IndexSettings> iss = new HashSet<>();
        for (IndexSettings is : factoryWeakHashMap.keySet()) {
            if (is.getIndex().getName().equals(settings.getIndex().getName())) {
                factoryWeakHashMap.get(is).closeSchedule();
                iss.add(is);
            }
        }
        for (IndexSettings is : iss) {
            factoryWeakHashMap.remove(is);
        }
//        if (factoryWeakHashMap.containsKey(settings)){
//            factoryWeakHashMap.get(settings).closeSchedule();
//            factoryWeakHashMap.remove(settings);
//        }
    }
}
