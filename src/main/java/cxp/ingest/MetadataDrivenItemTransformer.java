package cxp.ingest;

import org.joda.time.LocalDateTime;
import org.springframework.expression.ExpressionParser;

import java.util.List;

/**
 * Created by markmo on 13/06/15.
 */
public interface MetadataDrivenItemTransformer {

    <T> List<CustomerEvent> transform(T item) throws Exception;

    LocalDateTime parseDate(String value);

    void setMetadataProvider(MetadataProvider metadataProvider);

    void setParser(ExpressionParser parser);
}
