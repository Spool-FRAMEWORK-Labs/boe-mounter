package software.example.spool.boe;

import software.spool.mounter.api.model.AggregatedRecord;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountAggregator;
import software.spool.mounter.api.port.PartitionedRecord;

import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public class ToBinaryFileMountAggregator implements MountAggregator<byte[]> {
    @Override
    public Stream<AggregatedRecord<byte[]>> aggregate(Stream<PartitionedRecord<GenericRecord>> records) {
        return records.map(r -> {
            try {
                byte[] bytesPDF = HTTPUtils.get(r.record().getNested("payload").getString("url_pdf"));
                byte[] bytesHTML = HTTPUtils.get(r.record().getNested("payload").getString("url_html"));
                byte[] bytesXML = HTTPUtils.get(r.record().getNested("payload").getString("url_xml"));
                return List.of(new AggregatedRecord<>(r.record(), bytesXML),
                        new AggregatedRecord<>(r.record(), bytesPDF),
                        new AggregatedRecord<>(r.record(), bytesHTML));
            } catch (Exception e) {
                return null;
            }
        }).filter(Objects::nonNull).flatMap(List::stream).filter(r -> Objects.nonNull(r.output()));
    }
}
