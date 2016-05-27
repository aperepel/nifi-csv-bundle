package org.apache.nifi.processors.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractCSVProcessor extends AbstractProcessor {
    public static final String DEFAULT_SCHEMA_ATTR_PREFIX = "delimited.schema.column.";
    public static final AllowableValue VALUE_CSV = new AllowableValue("CSV", "CSV", "Standard comma-separated format.");
    public static final AllowableValue VALUE_EXCEL = new AllowableValue("EXCEL", "EXCEL", "Excel file format (using a comma as the value delimiter). Note that the actual " +
                                                                                                  "value delimiter used by Excel is locale dependent, it might be necessary to customize " +
                                                                                                  "this format to accommodate to your regional settings.");
    public static final AllowableValue VALUE_RFC4180 = new AllowableValue("RFC4180", "RFC4180", "Common Format and MIME Type for Comma-Separated Values (CSV) Files");
    public static final AllowableValue VALUE_TAB = new AllowableValue("TAB", "TAB", "Tab-delimited format.");
    public static final AllowableValue VALUE_MYSQL = new AllowableValue("MYSQL", "MYSQL", "Default MySQL format used " +
                                                                                                  "by the {@code SELECT INTO OUTFILE} and {@code LOAD DATA INFILE} operations.");
    public static final PropertyDescriptor PROP_FORMAT = new PropertyDescriptor.Builder()
                                                                 .name("Delimited Format")
                                                                 .description("Delimited content format")
                                                                 .required(true)
                                                                 .defaultValue(VALUE_EXCEL.getValue())
                                                                 .allowableValues(VALUE_EXCEL, VALUE_CSV, VALUE_RFC4180, VALUE_TAB, VALUE_MYSQL)
                                                                 .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                                                                 .build();
    public static final PropertyDescriptor PROP_DELIMITER = new PropertyDescriptor.Builder()
                                                                    .name("Column Delimiter")
                                                                    .description("Column Delimiter")
                                                                    .required(false)
                                                                    .expressionLanguageSupported(true)
                                                                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                                                                    .build();

    protected static final Map<String, CSVFormat> supportedFormats = new HashMap<>();

    static {
        // we are using more user-friendly names in the UI
        supportedFormats.put(VALUE_EXCEL.getValue(), CSVFormat.EXCEL); // our default
        supportedFormats.put(VALUE_CSV.getValue(), CSVFormat.DEFAULT);
        supportedFormats.put(VALUE_TAB.getValue(), CSVFormat.TDF);
        supportedFormats.put(VALUE_RFC4180.getValue(), CSVFormat.RFC4180);
        supportedFormats.put(VALUE_MYSQL.getValue(), CSVFormat.MYSQL);
    }

    protected CSVFormat buildFormat(String format, String delimiter, boolean withHeader, String customHeader) {
        CSVFormat csvFormat = supportedFormats.get(format);
        if (csvFormat == null) {
            throw new IllegalArgumentException("Unsupported delimited format: " + format);
        }

        if (withHeader & customHeader != null) {
            csvFormat = csvFormat.withSkipHeaderRecord(true);
            csvFormat = csvFormat.withHeader(customHeader);
        } else if (withHeader & customHeader == null) {
            csvFormat = csvFormat.withHeader();
        }

        // don't use isNotBlank() here, as it strips tabs too
        if (StringUtils.isNotEmpty(delimiter)) {
            csvFormat = csvFormat.withDelimiter(delimiter.charAt(0));
        }
        return csvFormat;
    }
}
