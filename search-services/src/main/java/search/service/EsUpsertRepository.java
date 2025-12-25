package search.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import org.springframework.stereotype.Repository;
import search.v1.Artist;
import search.v1.EntityType;
import search.v1.Playlist;
import search.v1.Track;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Repository
public class EsUpsertRepository {

    private final ElasticsearchClient es;

    public EsUpsertRepository(ElasticsearchClient es) {
        this.es = es;
    }

    // Пишем через alias (у тебя они есть: tracks, artists, playlists)
    public String indexName(EntityType type) {
        return switch (type) {
            case TRACK -> "tracks";
            case ARTIST -> "artists";
            case PLAYLIST -> "playlists";
            default -> throw new IllegalArgumentException("Unsupported type: " + type);
        };
    }

    public IndexResponse upsertTrack(Track t) throws IOException {
        return es.index(i -> i.index(indexName(EntityType.TRACK))
                .id(t.getId())
                .document(ProtoMap.toMap(t)));
    }

    public IndexResponse upsertArtist(Artist a) throws IOException {
        return es.index(i -> i.index(indexName(EntityType.ARTIST))
                .id(a.getId())
                .document(ProtoMap.toMap(a)));
    }

    public IndexResponse upsertPlaylist(Playlist p) throws IOException {
        return es.index(i -> i.index(indexName(EntityType.PLAYLIST))
                .id(p.getId())
                .document(ProtoMap.toMap(p)));
    }

    public BulkResponse bulkUpsert(List<Map<String, Object>> docs,
                                   List<EntityType> types,
                                   List<String> ids) throws IOException {

        List<BulkOperation> ops = new ArrayList<>();

        for (int i = 0; i < docs.size(); i++) {
            EntityType type = types.get(i);
            String index = indexName(type);
            String id = ids.get(i);
            Map<String, Object> doc = docs.get(i);

            IndexOperation<Map<String, Object>> idxOp = new IndexOperation.Builder<Map<String, Object>>()
                    .index(index)
                    .id(id)
                    .document(doc)
                    .build();

            ops.add(new BulkOperation.Builder().index(idxOp).build());
        }

        BulkRequest req = new BulkRequest.Builder().operations(ops).build();
        return es.bulk(req);
    }
}
