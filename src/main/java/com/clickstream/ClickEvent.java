package com.clickstream;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parser/validator for one line of the Kaggle
 * "eCommerce behavior data from multi category store" CSV.
 *
 * Columns (9): event_time, event_type, product_id, category_id,
 *              category_code, brand, price, user_id, user_session
 *
 * A record is "bad" (counted as an error) if the FIRST failing check below
 * fires, in this order: field count, timestamp, event type, product id,
 * user id, price. Each bad record therefore yields exactly one Problem.
 */
public final class ClickEvent {

    /** Hadoop counter group used by all jobs. */
    public static final String COUNTER_GROUP = "Clickstream";

    public static final int EXPECTED_FIELDS = 9;

    public enum Problem {
        WRONG_FIELD_COUNT,
        BAD_TIMESTAMP,
        BAD_EVENT_TYPE,
        BAD_PRODUCT_ID,
        BAD_USER_ID,
        BAD_PRICE
    }

    private static final Pattern TS = Pattern.compile(
            "^(\\d{4})-(\\d{2})-(\\d{2}) (\\d{2}):(\\d{2}):(\\d{2}) UTC$");
    private static final Pattern DIGITS = Pattern.compile("^\\d+$");
    private static final Pattern PRICE = Pattern.compile("^-?\\d+(\\.\\d+)?$");

    /** yyyy-MM-dd, or null when the timestamp could not be parsed. */
    public String date;
    /** HH (00-23), or null when the timestamp could not be parsed. */
    public String hour;
    public String eventType;
    public String productId;
    public String categoryCode;
    public String brand;
    public String userId;
    /** null when the record is valid. */
    public Problem problem;

    private ClickEvent() { }

    public static boolean isHeader(String line) {
        return line.startsWith("event_time");
    }

    public static ClickEvent parse(String rawLine) {
        String line = rawLine;
        if (line.endsWith("\r")) {
            line = line.substring(0, line.length() - 1);
        }
        ClickEvent e = new ClickEvent();
        String[] f = line.split(",", -1);

        if (f.length != EXPECTED_FIELDS) {
            e.problem = Problem.WRONG_FIELD_COUNT;
            return e;
        }

        Matcher m = TS.matcher(f[0]);
        if (m.matches()) {
            int month = Integer.parseInt(m.group(2));
            int day = Integer.parseInt(m.group(3));
            int hh = Integer.parseInt(m.group(4));
            int mm = Integer.parseInt(m.group(5));
            int ss = Integer.parseInt(m.group(6));
            if (month >= 1 && month <= 12 && day >= 1 && day <= 31
                    && hh <= 23 && mm <= 59 && ss <= 59) {
                e.date = m.group(1) + "-" + m.group(2) + "-" + m.group(3);
                e.hour = m.group(4);
            }
        }
        if (e.date == null) {
            e.problem = Problem.BAD_TIMESTAMP;
            return e;
        }

        e.eventType = f[1];
        if (!(e.eventType.equals("view") || e.eventType.equals("cart")
                || e.eventType.equals("remove_from_cart") || e.eventType.equals("purchase"))) {
            e.problem = Problem.BAD_EVENT_TYPE;
            return e;
        }

        e.productId = f[2];
        if (!DIGITS.matcher(e.productId).matches()) {
            e.problem = Problem.BAD_PRODUCT_ID;
            return e;
        }

        e.userId = f[7];
        if (!DIGITS.matcher(e.userId).matches()) {
            e.problem = Problem.BAD_USER_ID;
            return e;
        }

        String price = f[6];
        if (!PRICE.matcher(price).matches() || Double.parseDouble(price) <= 0.0) {
            e.problem = Problem.BAD_PRICE;
            return e;
        }

        e.categoryCode = f[4].isEmpty() ? "NA" : f[4];
        e.brand = f[5].isEmpty() ? "NA" : f[5];
        return e;
    }
}
