package search.service.es;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Component
public class EsSearchRepository {

    private final ElasticsearchClient client;

    public EsSearchRepository(
            @Value("${elasticsearch.url}") String esUrl,
            @Value("${elasticsearch.connect-timeout-ms:2000}") int connectTimeoutMs,
            @Value("${elasticsearch.socket-timeout-ms:5000}") int socketTimeoutMs
    ) {
        // RestClient builder with timeouts
        RestClient restClient = RestClient.builder(HttpHost.create(esUrl))
                .setRequestConfigCallback(rcb -> rcb
                        .setConnectTimeout(connectTimeoutMs)
                        .setSocketTimeout(socketTimeoutMs)
                )
                .build();

        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        this.client = new ElasticsearchClient(transport);
    }

    public List<RawResult> searchTracks(String query, int size) throws IOException {
        SearchResponse<JsonData> resp = client.search(s -> s
                        .index(EsIndex.TRACKS)
                        .size(size)
                        .query(q -> q
                                .multiMatch(mm -> mm
                                        .query(query)
                                        .fields("title^3", "artistName^2", "albumTitle")
                                )
                        ),
                JsonData.class
        );

        return toRawResults(resp, RawResult.Type.TRACK);
    }

    public List<RawResult> searchArtists(String query, int size) throws IOException {
        SearchResponse<JsonData> resp = client.search(s -> s
                        .index(EsIndex.ARTISTS)
                        .size(size)
                        .query(q -> q
                                .multiMatch(mm -> mm
                                        .query(query)
                                        .fields("name^3", "aliases")
                                )
                        ),
                JsonData.class
        );

        return toRawResults(resp, RawResult.Type.ARTIST);
    }

    public List<RawResult> searchPlaylists(String query, int size) throws IOException {
        SearchResponse<JsonData> resp = client.search(s -> s
                        .index(EsIndex.PLAYLISTS)
                        .size(size)
                        .query(q -> q
                                .multiMatch(mm -> mm
                                        .query(query)
                                        .fields("title^3", "description", "ownerName")
                                )
                        ),
                JsonData.class
        );

        return toRawResults(resp, RawResult.Type.PLAYLIST);
    }

    private static List<RawResult> toRawResults(SearchResponse<JsonData> resp, RawResult.Type type) {
        if (resp.hits() == null || resp.hits().hits() == null) return List.of();

        return resp.hits().hits().stream()
                .map(hit -> mapHit(hit, type))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private static RawResult mapHit(Hit<JsonData> hit, RawResult.Type type) {
        Map<String, Object> src = safeToMap(hit.source());
        String id = valueAsString(src.get("id"));
        if (id == null || id.isBlank()) {
            id = hit.id(); // fallback
        }

        String title;
        String subtitle;

        switch (type) {
            case TRACK -> {
                title = valueAsString(src.get("title"));
                subtitle = valueAsString(src.get("artistName"));
            }
            case ARTIST -> {
                title = valueAsString(src.get("name"));
                subtitle = valueAsString(src.get("country"));
            }
            case PLAYLIST -> {
                title = valueAsString(src.get("title"));
                subtitle = valueAsString(src.get("ownerName"));
            }
            default -> {
                return null;
            }
        }

        float score = hit.score() == null ? 0.0f : hit.score().floatValue();
        return new RawResult(type, id, defaultString(title), defaultString(subtitle), score);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> safeToMap(JsonData source) {
        if (source == null) return Collections.emptyMap();
        try {
            return source.to(Map.class);
        } catch (Exception e) {
            return Collections.emptyMap();
        }
    }

    private static String valueAsString(Object v) {
        return v == null ? null : String.valueOf(v);
    }

    private static String defaultString(String s) {
        return s == null ? "" : s;
    }

    // Внутренняя модель результата из ES (не gRPC)
    public record RawResult(Type type, String id, String title, String subtitle, float score) {
        public enum Type {TRACK, ARTIST, PLAYLIST}
    }
}
