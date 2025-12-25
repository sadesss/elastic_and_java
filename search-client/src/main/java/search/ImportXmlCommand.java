package search;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import xml.ArtistsXml;
import xml.TracksXml;
import search.v1.Artist;
import search.v1.BulkUpsertRequest;
import search.v1.BulkUpsertResponse;
import search.v1.Entity;
import search.v1.SearchServiceGrpc;
import search.v1.Track;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public class ImportXmlCommand {

    public static void importArtists(String file, String host, int port) throws Exception {
        XmlMapper mapper = new XmlMapper();
        String xml = Files.readString(Path.of(file));
        ArtistsXml root = mapper.readValue(xml, ArtistsXml.class);

        BulkUpsertRequest.Builder req = BulkUpsertRequest.newBuilder();

        if (root.artists != null) {
            for (ArtistsXml.ArtistXml a : root.artists) {
                Artist.Builder ab = Artist.newBuilder()
                        .setId(nz(a.id))
                        .setName(nz(a.name))
                        .setCountry(nz(a.country))
                        .setPopularity(a.popularity != null ? a.popularity : 0)
                        .setCreatedAt(nz(a.createdAt));

                if (a.tags != null) ab.addAllTags(a.tags);

                req.addEntities(Entity.newBuilder().setArtist(ab.build()).build());
            }
        }

        BulkUpsertResponse resp = callBulk(req.build(), host, port);
        printBulk(resp);
    }

    public static void importTracks(String file, String host, int port) throws Exception {
        XmlMapper mapper = new XmlMapper();
        String xml = Files.readString(Path.of(file));
        TracksXml root = mapper.readValue(xml, TracksXml.class);

        BulkUpsertRequest.Builder req = BulkUpsertRequest.newBuilder();

        if (root.tracks != null) {
            for (TracksXml.TrackXml t : root.tracks) {
                Track.Builder tb = Track.newBuilder()
                        .setId(nz(t.id))
                        .setTitle(nz(t.title))
                        .setArtistId(nz(t.artistId))
                        .setArtistName(nz(t.artistName))
                        .setAlbumTitle(nz(t.albumTitle))
                        .setGenre(nz(t.genre))
                        .setYear(t.year != null ? t.year : 0)
                        .setDurationSec(t.durationSec != null ? t.durationSec : 0)
                        .setPopularity(t.popularity != null ? t.popularity : 0)
                        .setCreatedAt(nz(t.createdAt));

                if (t.tags != null) tb.addAllTags(t.tags);

                req.addEntities(Entity.newBuilder().setTrack(tb.build()).build());
            }
        }

        BulkUpsertResponse resp = callBulk(req.build(), host, port);
        printBulk(resp);
    }

    private static BulkUpsertResponse callBulk(BulkUpsertRequest req, String host, int port) throws Exception {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        try {
            SearchServiceGrpc.SearchServiceBlockingStub stub = SearchServiceGrpc.newBlockingStub(channel);
            return stub.bulkUpsert(req);
        } finally {
            channel.shutdown();
            channel.awaitTermination(3, TimeUnit.SECONDS);
        }
    }

    private static void printBulk(BulkUpsertResponse resp) {
        System.out.println("=== BULK UPSERT ===");
        System.out.printf("total=%d success=%d failed=%d%n", resp.getTotal(), resp.getSuccess(), resp.getFailed());
        resp.getErrorsList().forEach(e ->
                System.out.printf("ERR idx=%d type=%s id=%s msg=%s%n",
                        e.getItemIndex(), e.getType(), e.getId(), e.getMessage())
        );
        System.out.println("=== DONE ===");
    }

    private static String nz(String s) {
        return s == null ? "" : s;
    }
}
