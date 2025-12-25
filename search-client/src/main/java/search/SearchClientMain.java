package search;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import search.v1.EntityType;
import search.v1.SearchRequest;
import search.v1.SearchResponse;
import search.v1.SearchServiceGrpc;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

public class SearchClientMain {

    // args:
    // 0: query (например "metallica")
    // 1: size (например 10) - optional
    // 2: types (например "TRACK,ARTIST") - optional
    // 3: host (например "localhost") - optional
    // 4: port (например 9090) - optional
    public static void main(String[] args) throws Exception {

        if (args.length >= 2 && "import-artists".equalsIgnoreCase(args[0])) {
            // args: import-artists <file> [host] [port]
            String file = args[1];
            String host = args.length > 2 ? args[2] : "localhost";
            int port = args.length > 3 ? Integer.parseInt(args[3]) : 9090;
            ImportXmlCommand.importArtists(file, host, port);
            return;
        }

        if (args.length >= 2 && "import-tracks".equalsIgnoreCase(args[0])) {
            // args: import-tracks <file> [host] [port]
            String file = args[1];
            String host = args.length > 2 ? args[2] : "localhost";
            int port = args.length > 3 ? Integer.parseInt(args[3]) : 9090;
            ImportXmlCommand.importTracks(file, host, port);
            return;
        }

        String query = args.length > 0 ? args[0] : "metallica";
        int size = args.length > 1 ? parseIntOrDefault(args[1], 10) : 10;
        String typesArg = args.length > 2 ? args[2] : "";
        String host = args.length > 3 ? args[3] : "localhost";
        int port = args.length > 4 ? parseIntOrDefault(args[4], 9090) : 9090;

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext()
                .build();

        try {
            SearchServiceGrpc.SearchServiceBlockingStub stub = SearchServiceGrpc.newBlockingStub(channel);

            SearchRequest.Builder req = SearchRequest.newBuilder()
                    .setQuery(query)
                    .setSize(size);

            // types: TRACK,ARTIST,PLAYLIST
            if (!typesArg.isBlank()) {
                for (String t : typesArg.split(",")) {
                    EntityType et = parseType(t.trim());
                    if (et != EntityType.ENTITY_TYPE_UNSPECIFIED) {
                        req.addTypes(et);
                    }
                }
            }

            SearchResponse resp = stub.search(req.build());

            System.out.println("=== gRPC SEARCH RESULTS ===");
            resp.getResultsList().forEach(r ->
                    System.out.printf("%s | %s | %s | score=%.3f%n",
                            r.getType(), r.getTitle(), r.getSubtitle(), r.getScore())
            );
            System.out.println("=== DONE ===");

        } finally {
            channel.shutdown();
            channel.awaitTermination(3, TimeUnit.SECONDS);
        }
    }

    private static int parseIntOrDefault(String s, int def) {
        try {
            return Integer.parseInt(s);
        } catch (Exception e) {
            return def;
        }
    }

    private static EntityType parseType(String s) {
        if (s == null) return EntityType.ENTITY_TYPE_UNSPECIFIED;
        String x = s.toUpperCase(Locale.ROOT);
        return switch (x) {
            case "TRACK" -> EntityType.TRACK;
            case "ARTIST" -> EntityType.ARTIST;
            case "PLAYLIST" -> EntityType.PLAYLIST;
            default -> EntityType.ENTITY_TYPE_UNSPECIFIED;
        };
    }
}
