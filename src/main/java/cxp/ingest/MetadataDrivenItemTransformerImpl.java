package cxp.ingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Predicate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by markmo on 7/04/15.
 */
public class MetadataDrivenItemTransformerImpl implements MetadataDrivenItemTransformer {

    private static final Log log = LogFactory.getLog(MetadataDrivenItemTransformerImpl.class);

    private static final String[] charTypes = new String[] { "STRING", "TEXT", "NONE" };

    static String[] dateFormats = new String[] {
            // pattern                      // example
            "yyyy-MM-dd'T'HH:mm:ss.SSSZ",   // 2001-07-04T12:08:56.235-0700
            "yyyy-MM-dd'T'HH:mm:ss.SSSXXX", // 2001-07-04T12:08:56.235-07:00
            "yyyy-MM-dd'T'HH:mm:ss.SSSSSS", // 2001-07-04T12:08:56.235000
            "yyyy-MM-dd HH:mm:ss.SSSZ",     // 2001-07-04 12:08:56.235-0700
            "yyyy-MM-dd HH:mm:ss.SSSXXX",   // 2001-07-04 12:08:56.235-07:00
            "yyyy-MM-dd HH:mm:ss.SSSSSS",   // 2001-07-04 12:08:56.235000
            "yyyyMMdd HH:mm:ss",            // 20010704 12:08:56
            "EEE, MMM d, ''yy",             // Wed, Jul 4, '01
            "EEE, MMM d, yyyy",             // Wed, Jul 4, 2001
            "yyyy.MM.dd",                   // 2001.07.04
            "yyyy-MM-dd",                   // 2001-07-04
            "yyyy/MM/dd",                   // 2001/07/04
            "dd.MM.yyyy",                   // 04.07.2001
            "dd-MM-yyyy",                   // 04-07-2001
            "dd/MM/yyyy",                   // 04/07/2001
            "MM.dd.yyyy",                   // 07.04.2001
            "MM-dd-yyyy",                   // 07-04-2001
            "MM/dd/yyyy",                   // 07/04/2001
            "dd.MM.yy",                     // 04.07.01
            "dd-MM-yy",                     // 04-07-01
            "dd/MM/yy",                     // 04/07/01
            "MM.dd.yy",                     // 07.04.01
            "MM-dd-yy",                     // 07-04-01
            "MM/dd/yy",                     // 07/04/01
            "dd/MMM/yy",                    // 03/APR/15
            "yyyy-MM-dd",
            "yyyy-MM-dd'T'HH",
            "yyyy-MM-dd HH",
            "yyyy-MM-dd'T'HH:mm",
            "yyyy-MM-dd HH:mm",
            "yyyy-MM-dd'T'HH:mm:ss",
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm:ss.SSS",
            "yyyy-MM-dd HH:mm:ss.SSS",
            "yyyy-MM-dd'T'HH:mm:ss Z",
            "yyyy-MM-dd HH:mm:ss Z"
    };

    private MetadataProvider metadataProvider;

    private ExpressionParser parser;

    private StandardEvaluationContext evaluationContext;

    private List<String> dfList;

    private FileDataset fileDataset = null;

    private Map<Integer, Expression> filterExpressionMap = null;

    private Map<Integer, Expression> customerId1ExpressionMap = null;

    private Map<Integer, Expression> customerId2ExpressionMap = null;

    private Map<Integer, Expression> valueExpressionMap = null;

    private Map<Integer, Expression> tsExpressionMap = null;

    private Map<Integer, Expression> eventPropertyTypeExpressionMap = null;

    private Map<Integer, Boolean> shouldFilterMap;

    private Long jobId;

    private ObjectMapper mapper;

    private Pattern timezoneSuffixPattern;

    private boolean json = false;

    public MetadataDrivenItemTransformerImpl() {
        this.dfList = Arrays.asList(dateFormats);
        shouldFilterMap = new HashMap<Integer, Boolean>();
        mapper = new ObjectMapper();
        timezoneSuffixPattern = Pattern.compile("([a-zA-Z_\\-]+(/[a-zA-Z_\\-]+)?)$");
    }

    public <T> List<CustomerEvent> transform(T item) throws Exception {
        Assert.notNull(fileDataset);
        Assert.notNull(shouldFilterMap);

        if (log.isDebugEnabled()) {
            log.debug(".");
        }

        metadataProvider.incrementRecordsProcessed(1);

        List<CustomerEvent> events = new ArrayList<CustomerEvent>();

        if (item == null) return events;

        if (fileDataset.getEventTypes() != null) {

            EvaluationContext ctx;
            if (json) {
                ctx = evaluationContext;
                ctx.setVariable("json", item);
            } else {
                ctx = new StandardEvaluationContext(item);
            }

            for (EventType eventType : fileDataset.getEventTypes()) {
                Integer eventTypeId = eventType.getId();
                boolean shouldInclude = true;

                if (json && isJsonPathExpr(eventType.getFilterExpression())) {
                    shouldInclude = JsonPathUtils.evaluate(item, eventType.getFilterExpression());
                }
                else if (shouldFilterMap.get(eventTypeId)) {
                    shouldInclude = filterExpressionMap.get(eventTypeId).getValue(ctx, Boolean.class);
                }

                // do not process if record matches the filter condition
                if (shouldInclude) {

                    if (json && !hasNoValue(eventType.getNestedDocumentExpression())) {
                        List<Map<String, Object>> documents = JsonPathUtils.evaluate(item, eventType.getNestedDocumentExpression());

                        if (documents == null || documents.isEmpty()) {
                            if (log.isDebugEnabled()) {
                                log.debug("No nested documents found");
                            }
                            metadataProvider.writeError(
                                    new Error("No nested documents found", true,
                                            eventTypeId, fileDataset.getId(), null,
                                            mapper.writeValueAsString(item)));
                            return events;
                        }

                        for (Map<String, Object> document : documents) {
                            events.addAll(extractEvents(ctx, eventType, document));
                        }
                    }
                    else {
                        events.addAll(extractEvents(ctx, eventType, item));
                    }
                }
                else {
                    metadataProvider.incrementRecordsSkipped(1);
                }
            }
        }
        return events;
    }

    private <T> List<CustomerEvent> extractEvents(EvaluationContext ctx,
                                                  EventType eventType,
                                                  T item) throws Exception {
        List<CustomerEvent> events = new ArrayList<CustomerEvent>();
        Integer eventTypeId = eventType.getId();

        // extract source key
        Map<String, Object> naturalKey = new HashMap<String, Object>();
        if (fileDataset.getNaturalKeyColumns() != null) {
            for (FileColumn column : fileDataset.getNaturalKeyColumns()) {
                Object keyValue = null;
                if (json) {
                    String expr = String.format("$['%s']", column.getName());
                    keyValue = JsonPathUtils.evaluate(item, expr);
                } else if (item instanceof Map) {
                    keyValue = ((Map)item).get(column.getName());
                }
                naturalKey.put(column.getName(), keyValue);
            }
        }
        String sourceKey = mapper.writeValueAsString(naturalKey);

        Integer customerIdTypeId = null;
        Object customerId = null;
        if (json && isJsonPathExpr(eventType.getCustomerIdExpression1())) {
            customerIdTypeId = eventType.getCustomerIdType1Id();
            customerId = JsonPathUtils.evaluate(item, eventType.getCustomerIdExpression1());
        } else if (customerId1ExpressionMap != null && customerId1ExpressionMap.containsKey(eventTypeId)) {
            customerIdTypeId = eventType.getCustomerIdType1Id();
            customerId = customerId1ExpressionMap.get(eventTypeId).getValue(item);
        }
        if (hasNoValue(customerId)) {
            if (json && isJsonPathExpr(eventType.getCustomerIdExpression2())) {
                customerIdTypeId = eventType.getCustomerIdType2Id();
                customerId = JsonPathUtils.evaluate(item, eventType.getCustomerIdExpression2());
            } else if (customerId2ExpressionMap != null && customerId2ExpressionMap.containsKey(eventTypeId)) {
                customerIdTypeId = eventType.getCustomerIdType2Id();
                customerId = customerId2ExpressionMap.get(eventTypeId).getValue(item);
            }
        }
        if (hasNoValue(customerId)) {
            // TODO
            if (log.isDebugEnabled()) {
                log.debug("No Customer ID found");
            }
            metadataProvider.writeError(
                    new Error("No Customer ID found", true,
                            eventTypeId, fileDataset.getId(), sourceKey,
                            mapper.writeValueAsString(item)));
            return events;
        }

        String value = null;
        if (json && isJsonPathExpr(eventType.getValueExpression())) {
            value = JsonPathUtils.evaluate(item, eventType.getValueExpression());
        } else if (valueExpressionMap != null && valueExpressionMap.containsKey(eventTypeId)) {
            value = (String) valueExpressionMap.get(eventTypeId).getValue(item);
        }
        if (hasNoValue(value)) {
            // TODO
            if (log.isDebugEnabled()) {
                log.debug("No value set");
            }
            value = eventType.getName();
        } else {
            value = value.trim();
        }

        LocalDateTime dt = null;
        String ts = null;
        if (json && isJsonPathExpr(eventType.getTsExpression())) {
            ts = JsonPathUtils.evaluate(item, eventType.getTsExpression());
        } else if (tsExpressionMap != null && tsExpressionMap.containsKey(eventTypeId)) {
            ts = (String) tsExpressionMap.get(eventTypeId).getValue(item);
        }
        if (hasNoValue(ts)) {
            // TODO
            if (log.isDebugEnabled()) {
                log.debug("No timestamp expression or does not evaluate");
            }
            metadataProvider.writeError(
                    new Error("No timestamp expression or does not evaluate", true,
                            eventTypeId, fileDataset.getId(), sourceKey,
                            mapper.writeValueAsString(item)));
            return events;

        } else {
            String format = eventType.getDatetimeFormat();
            if (hasNoValue(format)) {
                dt = parseDate(ts);
            } else {
                try {
                    DateTimeFormatter formatter = DateTimeFormat.forPattern(format);
                    dt = LocalDateTime.parse(ts, formatter);
                } catch (Exception e) {
                    if (log.isDebugEnabled()) {
                        log.debug("Cannot parse '" + ts + "' using given format '" + format + "'");
                    }
                    dt = parseDate(ts);
                }
            }
            if (dt == null) {
                if (log.isDebugEnabled()) {
                    log.debug("Cannot recognize timestamp format '" + ts + "'");
                }
                metadataProvider.writeError(
                        new Error("Cannot recognize timestamp format '" + ts + "'", true,
                                eventTypeId, fileDataset.getId(), sourceKey,
                                mapper.writeValueAsString(item)));
                return events;
            }
        }

        if (customerId instanceof String) {
            CustomerEvent event = createEvent(customerIdTypeId, (String)customerId, eventType, value, dt, sourceKey, item, ctx);
            events.add(event);
            metadataProvider.incrementEventsCreated(1);
        } else {
            for (Object id : (List)customerId) {
                CustomerEvent event = createEvent(customerIdTypeId, (String)id, eventType, value, dt, sourceKey, item, ctx);
                events.add(event);
                metadataProvider.incrementEventsCreated(1);
            }
        }
        return events;
    }

    private <T> CustomerEvent createEvent(Integer customerIdTypeId,
                                      String customerId,
                                      EventType eventType,
                                      String value,
                                      LocalDateTime dt,
                                      String sourceKey,
                                      T item,
                                      EvaluationContext ctx) throws Exception {
        CustomerEvent event = new CustomerEvent(customerIdTypeId, customerId, eventType.getId(), eventType.getValueTypeName(), value, dt, jobId, sourceKey);
        if (fileDataset.getColumns() != null) {
            for (FileColumn column : fileDataset.getColumns()) {
                for (EventPropertyType eventPropertyType : column.getEventPropertyTypes()) {
                    Integer eventPropertyTypeId = eventPropertyType.getId();
                    Object val = null;
                    if (json && isJsonPathExpr(eventPropertyType.getMappingExpression())) {
                        val = JsonPathUtils.evaluate(item, eventPropertyType.getMappingExpression());
                    } else if (eventPropertyTypeExpressionMap != null && eventPropertyTypeExpressionMap.containsKey(eventPropertyTypeId)) {
                        val = eventPropertyTypeExpressionMap.get(eventPropertyTypeId).getValue(ctx);
                    }
                    String v = ((val == null || val instanceof List) ? "" : val.toString());
                    event.addProperty(new CustomerEventProperty(eventPropertyTypeId, eventPropertyType.getValueTypeName(), v));
                }
            }
        }
        return event;
    }

    // TODO
    // find a faster way
    public LocalDateTime parseDate(String value) {
        if (value == null) return null;
        String v = value.trim();
        if (v.isEmpty()) return null;
        Matcher matcher = timezoneSuffixPattern.matcher(v);
        if (matcher.find()) {
            v = v.substring(0, matcher.start()).trim();
        }
        int i = 0;
        for (String format : dfList) {
            try {
                if (log.isDebugEnabled()) {
                    log.debug("source ts: " + v);
                    log.debug("dt format: " + format);
                }
                DateTimeFormatter formatter = DateTimeFormat.forPattern(format);
                LocalDateTime dt = LocalDateTime.parse(v, formatter);
                // Move valid format to top of list
                if (i > 0) {
                    dfList = rearrange(dfList, format);
                }
                if (log.isDebugEnabled()) {
                    log.debug("parsed ts: " + dt);
                }

                return dt;

            } catch (IllegalArgumentException e) {
                // ignore
            }
            i += 1;
        }
        return null;
    }

    public void setMetadataProvider(MetadataProvider metadataProvider) {
        this.metadataProvider = metadataProvider;
        String[] dfs = metadataProvider.getDateFormats();
        if (dfs != null) {
            dateFormats = dfs;
            dfList = Arrays.asList(dateFormats);
        }
        if (fileDataset == null && parser != null) setExpressions();
    }

    public void setParser(ExpressionParser parser) {
        this.parser = parser;
        if (fileDataset == null && metadataProvider != null) setExpressions();
    }

    private void setExpressions() {
        if (metadataProvider.isJson()) {
            json = true;
            evaluationContext = new StandardEvaluationContext();
            try {
                evaluationContext.registerFunction("jsonPath", JsonPathUtils.class.getDeclaredMethod("evaluate", Object.class, String.class, Predicate[].class));
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e.getMessage(), e);
            }
        }
        if (metadataProvider.getFileDataset() != null) {
            fileDataset = metadataProvider.getFileDataset();

            if (fileDataset.getEventTypes() != null) {
                for (EventType eventType : fileDataset.getEventTypes()) {
                    Integer eventTypeId = eventType.getId();
                    if (hasNoValue(eventTypeId)) {
                        throw new RuntimeException("No event type ID");
                    }
                    String filterExpr = eventType.getFilterExpression();
                    if (hasNoValue(filterExpr)) {
                        shouldFilterMap.put(eventTypeId, false);
                    } else if (!isJsonPathExpr(filterExpr)) {
                        if (filterExpressionMap == null) {
                            filterExpressionMap = new HashMap<>();
                        }
                        filterExpressionMap.put(eventTypeId, parser.parseExpression(filterExpr));
                        shouldFilterMap.put(eventTypeId, true);
                    }

                    if (!hasNoValue(eventType.getCustomerIdType1Id()) && !hasNoValue(eventType.getCustomerIdExpression1()) &&
                            !isJsonPathExpr(eventType.getCustomerIdExpression1())) {
                        if (customerId1ExpressionMap == null) {
                            customerId1ExpressionMap = new HashMap<>();
                        }
                        customerId1ExpressionMap.put(eventTypeId, parser.parseExpression(eventType.getCustomerIdExpression1()));
                    }

                    if (!hasNoValue(eventType.getCustomerIdType2Id()) && !hasNoValue(eventType.getCustomerIdExpression2()) &&
                            !isJsonPathExpr(eventType.getCustomerIdExpression2())) {
                        if (customerId2ExpressionMap == null) {
                            customerId2ExpressionMap = new HashMap<>();
                        }
                        customerId2ExpressionMap.put(eventTypeId, parser.parseExpression(eventType.getCustomerIdExpression2()));
                    }

                    if (!hasNoValue(eventType.getValueExpression()) &&
                            !isJsonPathExpr(eventType.getValueExpression())) {
                        if (valueExpressionMap == null) {
                            valueExpressionMap = new HashMap<>();
                        }
                        valueExpressionMap.put(eventTypeId, parser.parseExpression(eventType.getValueExpression()));
                    }

                    if (!hasNoValue(eventType.getTsExpression()) &&
                            !isJsonPathExpr(eventType.getTsExpression())) {
                        if (tsExpressionMap == null) {
                            tsExpressionMap = new HashMap<>();
                        }
                        tsExpressionMap.put(eventTypeId, parser.parseExpression(eventType.getTsExpression()));
                    }
                }
            } else {
                // TODO
                log.warn("No event types defined for dataset " + fileDataset.getName());
            }

            if (fileDataset.getColumns() != null) {
                for (FileColumn column : fileDataset.getColumns()) {
                    for (EventPropertyType eventPropertyType : column.getEventPropertyTypes()) {
                        Integer eventPropertyTypeId = eventPropertyType.getId();
                        if (!hasNoValue(eventPropertyType.getMappingExpression()) &&
                                !isJsonPathExpr(eventPropertyType.getMappingExpression())) {
                            if (eventPropertyTypeExpressionMap == null) {
                                eventPropertyTypeExpressionMap = new HashMap<>();
                            }
                            eventPropertyTypeExpressionMap.put(eventPropertyTypeId, parser.parseExpression(eventPropertyType.getMappingExpression()));
                        }
                    }
                }
            } else {
                // TODO
                log.warn("No columns defined for dataset " + fileDataset.getName());
            }

            jobId = metadataProvider.getJobId();

        } else {
            // TODO
            log.warn("File dataset not found");
        }
    }

    private boolean hasNoValue(String str) {
        if (str == null) return true;
        String trimmed = str.trim();
        return (trimmed.isEmpty() || trimmed.matches("0+"));
    }

    private boolean hasNoValue(Integer i) {
        return (i == null || i == 0);
    }

    private boolean hasNoValue(Object obj) {
        return (obj == null || hasNoValue(obj.toString()));
    }

    private boolean isJsonPathExpr(String str) {
        return str != null && str.trim().startsWith("$");
    }

    private static <T> List<T> rearrange(List<T> items, T input) {
        int index = items.indexOf(input);
        List<T> copy;
        if (index >= 0) {
            copy = new ArrayList<T>(items.size());
            copy.addAll(items.subList(0, index));
            copy.add(0, items.get(index));
            copy.addAll(items.subList(index + 1, items.size()));
        } else {
            copy = new ArrayList<T>(items);
        }
        return copy;
    }
}
