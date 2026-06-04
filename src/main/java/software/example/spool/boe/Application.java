package software.example.spool.boe;

import software.spool.core.model.spool.SpoolNode;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.utils.polling.PollingPolicy;
import software.spool.infrastructure.PluginResolver;
import software.spool.infrastructure.adapter.dataLake.filesystem.FileSystemPartitionDiscovery;
import software.spool.infrastructure.adapter.dataLake.filesystem.FileSystemPartitionSplitter;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.bus.EventBusProvider;
import software.spool.infrastructure.spi.provider.dataLake.PartitionedReaderProvider;
import software.spool.infrastructure.spi.provider.datamart.DataMartWriterProvider;
import software.spool.mounter.api.MountMode;
import software.spool.mounter.api.Mounter;
import software.spool.mounter.api.adapter.AlwaysClosedWindowPolicy;
import software.spool.mounter.api.adapter.NoOpMountCheckpoint;
import software.spool.mounter.api.adapter.ThresholdScalingPolicy;
import software.spool.mounter.api.adapter.ThreadPoolPartitionDispatcher;
import software.spool.mounter.api.builder.MounterBuilderFactory;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.ExtensionResolver;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.utils.MounterErrorRouter;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Executors;

public class Application {

    public Application() throws IOException {
        SpoolNode node = SpoolNode.create();
        node.register(initializeMounter());
        node.start();
    }

    private static final String DATA_LAKE_PATH = "D:/spool/datalake";

    private Mounter initializeMounter() {
        return MounterBuilderFactory.<GenericRecord, byte[]>polling(PluginResolver.resolve(PartitionedReaderProvider.class, PluginConfiguration.builder().with("path", DATA_LAKE_PATH).build()))
                .aggregatingWith(new ExtractPayloadMountAggregator())
                .mount()
                    .onTarget(MountTarget.transformation("Synthea", buildTodayPartitionKey(), ExtensionResolver.fixed("json")))
                    .writingWith(PluginResolver.get(DataMartWriterProvider.class, "RAW_FILE_SYSTEM").create(PluginConfiguration.builder().with("path", DATA_LAKE_PATH).build()))
                    .partitioningWith((r, p, t) -> buildDatePartitionKeyFrom(r, (byte[]) p))
                    .and()
                .checkpoint()
                    .windowPolicy(new AlwaysClosedWindowPolicy())
                    .checkpoint(new NoOpMountCheckpoint())
                    .and()
                .scaling()
                    .discoveringWith(new FileSystemPartitionDiscovery(DATA_LAKE_PATH, MountMode.TRANSFORMATION))
                    .splittingWith(new FileSystemPartitionSplitter())
                    .scalingWith(ThresholdScalingPolicy.of(1, Integer.MAX_VALUE, 50_000))
                    .dispatchingWith(new ThreadPoolPartitionDispatcher(Executors.newFixedThreadPool(4)))
                    .and()
                .scheduling()
                    .pollingWith(PollingPolicy.ONCE)
                    .emittingWith(PluginResolver.resolve(EventBusProvider.class, PluginConfiguration.empty()))
                    .and()
                .observability()
                    .withErrorRouter(MounterErrorRouter.defaults(System.out::println))
                    .and()
                .build();
    }

    private static PartitionKey buildDatePartitionKeyFrom(GenericRecord record, byte[] payload) {
        // LocalDate publishDate = LocalDate.parse(record.getNested("payload").getString("publish_date"), DateTimeFormatter.BASIC_ISO_DATE);
        return PartitionKey.ofEntriesWithoutDate()
//                .with("year", String.valueOf(publishDate.getYear()))
//                .with("month", String.format("%02d", publishDate.getMonthValue()))
//                .with("day", String.format("%02d", publishDate.getDayOfMonth()))
                // .with("fileType", ContentType.detect(payload))
                .with("_type", record.getNested("payload").getString("_type"))
                .build();
    }

    private static PartitionKey buildTodayPartitionKey() {
        LocalDate today = LocalDate.now();
        return PartitionKey.ofEntriesWithoutDate()
                .with("year", String.valueOf(today.getYear()))
                .with("month", String.format("%02d", today.getMonthValue()))
                .with("day", String.format("%02d", today.getDayOfMonth()))
                .with("source", "synthea-api")
                .build();
    }

    public static void run() throws IOException {
        new Application();
    }
}