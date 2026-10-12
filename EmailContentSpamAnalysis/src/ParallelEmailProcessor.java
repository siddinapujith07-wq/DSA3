import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ParallelEmailProcessor {

    public static void processEmails(List<Email> emails) {
        if (emails == null || emails.isEmpty()) return;

        int threads = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(threads);

        try {
            for (Email email : emails) {
                executor.submit(() ->
                        TextPreprocessor.cleanText(email.getFullText())
                );
            }

            executor.shutdown();

            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }

        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();

        } finally {
            if (!executor.isShutdown()) {
                executor.shutdown();
            }
        }

        System.out.printf(
                "Parallel Processing: %d emails processed concurrently on %d CPU threads.%n",
                emails.size(),
                threads
        );
    }
}