package com.bellszhu.elasticsearch.plugin;

import static org.elasticsearch.plugins.AnalysisPlugin.requiresAnalysisSettings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;

import com.bellszhu.elasticsearch.plugin.synonym.analysis.DynamicSynonymGraphTokenFilterFactory;
import com.bellszhu.elasticsearch.plugin.synonym.analysis.DynamicSynonymTokenFilterFactory;


/**
 * @author bellszhu
 */
public class DynamicSynonymPlugin extends Plugin implements AnalysisPlugin {

    Logger logger = LogManager.getLogger("dynamic-synonym");

    @Override
    public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
        Map<String, AnalysisProvider<TokenFilterFactory>> extra = new HashMap<>();
        extra.put("dynamic_synonym", requiresAnalysisSettings(DynamicSynonymTokenFilterFactory::new));
        extra.put("dynamic_synonym_graph", requiresAnalysisSettings(DynamicSynonymGraphTokenFilterFactory::new));
        return extra;
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        super.onIndexModule(indexModule);
        IndexEventListener listener = new IndexEventListener() {
            @Override
            public void afterIndexRemoved(Index index, IndexSettings indexSettings, IndicesClusterStateService.AllocatedIndices.IndexRemovalReason reason) {
                IndexEventListener.super.afterIndexRemoved(index, indexSettings, reason);
            }

            @Override
            public void afterIndexShardDeleted(ShardId shardId, Settings indexSettings) {
                IndexEventListener.super.afterIndexShardDeleted(shardId, indexSettings);
                DynamicSynonymTokenFilterFactory.afterIndexShardDeleted();
            }

            @Override
            public  void afterIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
                IndexEventListener.super.afterIndexShardClosed(shardId, indexShard, indexSettings);
                DynamicSynonymTokenFilterFactory.afterIndexShardClosed(indexShard.getService().indexSettings());
                // 关闭、删除、开启（先关闭再重新初始化）
                logger.info("=====afterIndexShardClosed=======");
            }
        };
        indexModule.addIndexEventListener(listener);
    }
}