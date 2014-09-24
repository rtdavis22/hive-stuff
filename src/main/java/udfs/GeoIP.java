import java.io.File;
import java.io.IOException;

import com.maxmind.geoip.LookupService;
import com.maxmind.geoip.Location;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;

public class GeoIP extends UDF {
  private LookupService ls;

  public String evaluate(String ip, String property) {
    if (ls == null) {
      File f = new File("GeoIPCity.dat");
      if (!f.exists()) {
        return null;
      }
      try {
        ls = new LookupService(f , LookupService.GEOIP_MEMORY_CACHE);
      } catch (IOException ex) {
        return null;
      }
    }

    Location loc = ls.getLocation(ip);
    if (loc == null) {
      return null;
    }

    if (property.equals("COUNTRY_CODE")) {
        return loc.countryCode;
    } else if (property.equals("REGION")) {
        return loc.region;
    }

    return null;
  }
}
