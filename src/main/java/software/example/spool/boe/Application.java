package software.example.spool.boe;

import software.spool.core.model.spool.SpoolNode;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.utils.polling.PollingPolicy;
import software.spool.infrastructure.PluginResolver;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.bus.EventBusProvider;
import software.spool.infrastructure.spi.provider.dataLake.PartitionedReaderProvider;
import software.spool.infrastructure.spi.provider.datamart.DataMartWriterProvider;
import software.spool.mounter.api.Mounter;
import software.spool.mounter.api.adapter.AlwaysClosedWindowPolicy;
import software.spool.mounter.api.adapter.NoOpMountCheckpoint;
import software.spool.mounter.api.builder.MounterBuilderFactory;
import software.spool.mounter.api.model.ContentType;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.ExtensionResolver;
import software.spool.mounter.api.port.MountTarget;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class Application {

    public Application() throws IOException {
        SpoolNode node = SpoolNode.create();
        node.register(initializeMounter());
        node.start();
    }

    private Mounter initializeMounter() {
        return MounterBuilderFactory.polling(PluginResolver.resolve(PartitionedReaderProvider.class, PluginConfiguration.builder().with("path", "D:/spool/datalake").build()))
                .aggregatingWith(new ToBinaryFileMountAggregator())
                .onTarget(MountTarget.transformation("BOE-Files", buildTodayPartitionKey(), ExtensionResolver.auto()))
                .checkpoint(new NoOpMountCheckpoint())
                .partitionWindowPolicy(new AlwaysClosedWindowPolicy())
                .emittingWith(PluginResolver.resolve(EventBusProvider.class, PluginConfiguration.empty()))
                .pollingWith(PollingPolicy.ONCE)
                .writingWith(PluginResolver.get(DataMartWriterProvider.class, "RAW_FILE_SYSTEM").create(PluginConfiguration.builder().with("path", "D:/spool/datalake").build()))
                .partitioningWith((r, p, t) -> buildDatePartitionKeyFrom(r, p))
                .build();
    }

    private static PartitionKey buildDatePartitionKeyFrom(GenericRecord record, byte[] payload) {
        LocalDate publishDate = LocalDate.parse(record.getNested("payload").getString("publish_date"), DateTimeFormatter.BASIC_ISO_DATE);
        return PartitionKey.ofEntriesWithoutDate()
                .with("year", String.valueOf(publishDate.getYear()))
                .with("month", String.format("%02d", publishDate.getMonthValue()))
                .with("day", String.format("%02d", publishDate.getDayOfMonth()))
                .with("fileType", ContentType.detect(payload))
                .build();
    }

    private PartitionKey buildTodayPartitionKey() {
        LocalDate today = LocalDate.now();
        return PartitionKey.ofEntriesWithoutDate()
                .with("year", String.valueOf(today.getYear()))
                .with("month", String.format("%02d", today.getMonthValue()))
                .with("day", String.format("%02d", today.getDayOfMonth()))
                .with("source", "boe")
                .build();
    }

    public static void run() throws IOException {
        new Application();
    }
}