package search.service.client;

import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import search.v1.SearchRequest;
import search.v1.SearchResponse;
import search.v1.SearchServiceGrpc;

@Component
public class GrpcLocalClientTest {

    @GrpcClient("local-search")
    private SearchServiceGrpc.SearchServiceBlockingStub stub;

    // Запускаем тест только если передали аргумент "test-client"
    private boolean enabled = false;

    @Component
    static class ArgsCatcher implements ApplicationRunner {
        private final GrpcLocalClientTest parent;

        ArgsCatcher(GrpcLocalClientTest parent) {
            this.parent = parent;
        }

        @Override
        public void run(ApplicationArguments args) {
            parent.enabled = args.getNonOptionArgs().stream()
                    .anyMatch(a -> "test-client".equalsIgnoreCase(a));
            System.out.println("[GrpcLocalClientTest] enabled=" + parent.enabled + ", args=" + args.getNonOptionArgs());
        }
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        if (!enabled) return;

        try {
            SearchRequest req = SearchRequest.newBuilder()
                    .setQuery("metallica")
                    .setSize(10)
                    .build();

            SearchResponse resp = stub.search(req);

            System.out.println("=== gRPC SEARCH RESULTS ===");
            resp.getResultsList().forEach(r ->
                    System.out.printf("%s | %s | %s | score=%.3f%n",
                            r.getType(), r.getTitle(), r.getSubtitle(), r.getScore())
            );
            System.out.println("=== DONE ===");
        } catch (Exception e) {
            // ВАЖНО: не роняем всё приложение
            System.out.println("[GrpcLocalClientTest] gRPC call failed: " + e.getMessage());
            e.printStackTrace(System.out);
        }
    }
}
