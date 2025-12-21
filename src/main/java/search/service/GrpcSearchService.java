package search.service;

import search.service.es.EsSearchRepository;
import search.v1.EntityType;
import search.v1.SearchRequest;
import search.v1.SearchResponse;
import search.v1.SearchResult;
import search.v1.SearchServiceGrpc;
import io.grpc.Status;
import net.devh.boot.grpc.server.service.GrpcService;

import java.io.IOException;
import java.util.*;

@GrpcService
public class GrpcSearchService extends SearchServiceGrpc.SearchServiceImplBase {

    private final EsSearchRepository es;

    public GrpcSearchService(EsSearchRepository es) {
        this.es = es;
    }

    @Override
    public void search(SearchRequest request, io.grpc.stub.StreamObserver<SearchResponse> responseObserver) {
        String query = request.getQuery() == null ? "" : request.getQuery().trim();
        if (query.isEmpty()) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("query must not be empty").asRuntimeException());
            return;
        }

        int size = request.getSize() <= 0 ? 10 : request.getSize();
        size = Math.min(size, 50); // защита

        Set<EntityType> types = normalizeTypes(request.getTypesList());
        try {
            List<EsSearchRepository.RawResult> all = new ArrayList<>();

            // MVP: делаем 3 отдельных запроса и мерджим по score
            if (types.contains(EntityType.TRACK)) {
                all.addAll(es.searchTracks(query, size));
            }
            if (types.contains(EntityType.ARTIST)) {
                all.addAll(es.searchArtists(query, size));
            }
            if (types.contains(EntityType.PLAYLIST)) {
                all.addAll(es.searchPlaylists(query, size));
            }

            all.sort(Comparator.comparing(EsSearchRepository.RawResult::score).reversed());
            if (all.size() > size) {
                all = all.subList(0, size);
            }

            SearchResponse.Builder resp = SearchResponse.newBuilder();
            for (EsSearchRepository.RawResult r : all) {
                resp.addResults(SearchResult.newBuilder()
                        .setType(mapType(r.type()))
                        .setId(r.id())
                        .setTitle(r.title())
                        .setSubtitle(r.subtitle())
                        .setScore(r.score())
                        .build()
                );
            }

            responseObserver.onNext(resp.build());
            responseObserver.onCompleted();

        } catch (IOException e) {
            responseObserver.onError(Status.UNAVAILABLE
                    .withDescription("Elasticsearch unavailable: " + e.getMessage())
                    .asRuntimeException());
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal error: " + e.getMessage())
                    .asRuntimeException());
        }
    }

    private static Set<EntityType> normalizeTypes(List<EntityType> in) {
        if (in == null || in.isEmpty()) {
            return EnumSet.of(EntityType.TRACK, EntityType.ARTIST, EntityType.PLAYLIST);
        }
        EnumSet<EntityType> set = EnumSet.noneOf(EntityType.class);
        for (EntityType t : in) {
            if (t == EntityType.TRACK || t == EntityType.ARTIST || t == EntityType.PLAYLIST) {
                set.add(t);
            }
        }
        if (set.isEmpty()) {
            return EnumSet.of(EntityType.TRACK, EntityType.ARTIST, EntityType.PLAYLIST);
        }
        return set;
    }

    private static EntityType mapType(EsSearchRepository.RawResult.Type t) {
        return switch (t) {
            case TRACK -> EntityType.TRACK;
            case ARTIST -> EntityType.ARTIST;
            case PLAYLIST -> EntityType.PLAYLIST;
        };
    }
}
