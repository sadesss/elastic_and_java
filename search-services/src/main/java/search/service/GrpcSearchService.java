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

    private final EsUpsertRepository upsertRepository;

    public GrpcSearchService(EsSearchRepository searchRepository, EsUpsertRepository upsertRepository) {
        this.es = searchRepository;
        this.upsertRepository = upsertRepository;
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
    @Override
    public void upsert(search.v1.UpsertRequest request,
                       io.grpc.stub.StreamObserver<search.v1.UpsertResponse> responseObserver) {

        if (!request.hasEntity() || request.getEntity().getPayloadCase() == search.v1.Entity.PayloadCase.PAYLOAD_NOT_SET) {
            responseObserver.onNext(search.v1.UpsertResponse.newBuilder()
                    .setOk(false)
                    .setType(search.v1.EntityType.ENTITY_TYPE_UNSPECIFIED)
                    .setMessage("entity is required")
                    .build());
            responseObserver.onCompleted();
            return;
        }

        try {
            search.v1.Entity entity = request.getEntity();

            search.v1.UpsertResponse.Builder resp = search.v1.UpsertResponse.newBuilder().setOk(true);

            switch (entity.getPayloadCase()) {
                case TRACK -> {
                    var t = entity.getTrack();
                    var r = upsertRepository.upsertTrack(t);
                    resp.setType(search.v1.EntityType.TRACK)
                            .setId(t.getId())
                            .setIndex(upsertRepository.indexName(search.v1.EntityType.TRACK))
                            .setMessage(r.result().jsonValue());
                }
                case ARTIST -> {
                    var a = entity.getArtist();
                    var r = upsertRepository.upsertArtist(a);
                    resp.setType(search.v1.EntityType.ARTIST)
                            .setId(a.getId())
                            .setIndex(upsertRepository.indexName(search.v1.EntityType.ARTIST))
                            .setMessage(r.result().jsonValue());
                }
                case PLAYLIST -> {
                    var p = entity.getPlaylist();
                    var r = upsertRepository.upsertPlaylist(p);
                    resp.setType(search.v1.EntityType.PLAYLIST)
                            .setId(p.getId())
                            .setIndex(upsertRepository.indexName(search.v1.EntityType.PLAYLIST))
                            .setMessage(r.result().jsonValue());
                }
                default -> resp.setOk(false).setMessage("unsupported payload");
            }

            responseObserver.onNext(resp.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onNext(search.v1.UpsertResponse.newBuilder()
                    .setOk(false)
                    .setType(search.v1.EntityType.ENTITY_TYPE_UNSPECIFIED)
                    .setMessage("error: " + e.getMessage())
                    .build());
            responseObserver.onCompleted();
        }
    }
    @Override
    public void bulkUpsert(search.v1.BulkUpsertRequest request,
                           io.grpc.stub.StreamObserver<search.v1.BulkUpsertResponse> responseObserver) {

        var entities = request.getEntitiesList();
        int total = entities.size();

        if (total == 0) {
            responseObserver.onNext(search.v1.BulkUpsertResponse.newBuilder()
                    .setTotal(0).setSuccess(0).setFailed(0)
                    .build());
            responseObserver.onCompleted();
            return;
        }

        try {
            var docs = new java.util.ArrayList<java.util.Map<String, Object>>(total);
            var types = new java.util.ArrayList<search.v1.EntityType>(total);
            var ids = new java.util.ArrayList<String>(total);

            var errors = new java.util.ArrayList<search.v1.BulkError>();

            for (int i = 0; i < total; i++) {
                var e = entities.get(i);

                switch (e.getPayloadCase()) {
                    case TRACK -> {
                        var t = e.getTrack();
                        if (t.getId().isBlank()) {
                            errors.add(search.v1.BulkError.newBuilder()
                                    .setItemIndex(i).setType(search.v1.EntityType.TRACK).setId("")
                                    .setMessage("track.id is required")
                                    .build());
                            continue;
                        }
                        docs.add(ProtoMap.toMap(t));

                        types.add(search.v1.EntityType.TRACK);
                        ids.add(t.getId());
                    }
                    case ARTIST -> {
                        var a = e.getArtist();
                        if (a.getId().isBlank()) {
                            errors.add(search.v1.BulkError.newBuilder()
                                    .setItemIndex(i).setType(search.v1.EntityType.ARTIST).setId("")
                                    .setMessage("artist.id is required")
                                    .build());
                            continue;
                        }
                        docs.add(ProtoMap.toMap(a));
                        types.add(search.v1.EntityType.ARTIST);
                        ids.add(a.getId());
                    }
                    case PLAYLIST -> {
                        var p = e.getPlaylist();
                        if (p.getId().isBlank()) {
                            errors.add(search.v1.BulkError.newBuilder()
                                    .setItemIndex(i).setType(search.v1.EntityType.PLAYLIST).setId("")
                                    .setMessage("playlist.id is required")
                                    .build());
                            continue;
                        }
                        docs.add(ProtoMap.toMap(p));
                        types.add(search.v1.EntityType.PLAYLIST);
                        ids.add(p.getId());
                    }
                    default -> errors.add(search.v1.BulkError.newBuilder()
                            .setItemIndex(i).setType(search.v1.EntityType.ENTITY_TYPE_UNSPECIFIED).setId("")
                            .setMessage("payload not set")
                            .build());
                }
            }

            // bulk в ES
            var bulkResp = upsertRepository.bulkUpsert(docs, types, ids);

            int failed = 0;

            int success = docs.size() - failed;

            responseObserver.onNext(search.v1.BulkUpsertResponse.newBuilder()
                    .setTotal(total)
                    .setSuccess(success)
                    .setFailed(errors.size()) // можно считать как errors.size()
                    .addAllErrors(errors)
                    .build());
            responseObserver.onCompleted();

        } catch (Exception ex) {
            responseObserver.onNext(search.v1.BulkUpsertResponse.newBuilder()
                    .setTotal(total).setSuccess(0).setFailed(total)
                    .addErrors(search.v1.BulkError.newBuilder()
                            .setItemIndex(-1)
                            .setType(search.v1.EntityType.ENTITY_TYPE_UNSPECIFIED)
                            .setId("")
                            .setMessage("bulk error: " + ex.getMessage())
                            .build())
                    .build());
            responseObserver.onCompleted();
        }
    }

}
