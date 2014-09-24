import org.apache.hadoop.hive.ql.exec.UDF;

import org.joda.time.DateTime;
import org.joda.time.Days;

// Given a date and a modulus m, this UDF calculates another date 0-m days before d that is
// associated with a contiguous range of m dates. This can be used to calculate weekly (m = 7)
// and monthly (m = 28) cohorts.
public class DateFloor extends UDF {
    public String evaluate(String dtStr, int modulus) {
        DateTime dt = parseDate(dtStr);

        DateTime baseDt = new DateTime(1970, 1, 1, 0, 0);

        int diff = Days.daysBetween(baseDt.toDateMidnight(), dt.toDateMidnight()).getDays();

        return dateToString(dt.minusDays(diff%modulus));
    }

    private static String dateToString(DateTime dt) {
        return dt.getYear() + "-" +
               String.format("%02d", dt.getMonthOfYear()) + "-" +
               String.format("%02d", dt.getDayOfMonth());
    }

    private static DateTime parseDate(String dt) {
        String[] fields = dt.split("-");
        DateTime datetime = new DateTime(Integer.parseInt(fields[0]),
                            Integer.parseInt(fields[1]),
                            Integer.parseInt(fields[2]), 0, 0);
        return datetime;
    }
}
