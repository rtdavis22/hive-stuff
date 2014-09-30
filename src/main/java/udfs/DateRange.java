import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import org.joda.time.DateTime;

public class DateRange extends UDF {
    public List<String> evaluate(String dt1, String dt2) {
        DateTime start = parseDate(dt1);
        DateTime end = parseDate(dt2);

        List<String> ls = new ArrayList<String>();

        DateTime cur_date = start;
        while (!cur_date.equals(end)) {
            ls.add(dateToString(cur_date));
            cur_date = cur_date.plusDays(1);
        }

        return ls;
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
