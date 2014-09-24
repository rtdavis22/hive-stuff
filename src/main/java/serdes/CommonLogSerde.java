import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class CommonLogSerde implements SerDe {
    private StructTypeInfo rowTypeInfo;
    private ObjectInspector rowOI;
    private List<String> colNames;
    private List<Object> row = new ArrayList<Object>();

    @Override
    public void initialize(Configuration conf, Properties tbl) throws SerDeException {
        // Get a list of the table's column names.
        String colNamesStr = tbl.getProperty(serdeConstants.LIST_COLUMNS);
        colNames = Arrays.asList(colNamesStr.split(","));
  
        // Get a list of TypeInfos for the columns. This list lines up with
        // the list of column names.
        String colTypesStr = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        List<TypeInfo> colTypes = TypeInfoUtils.getTypeInfosFromTypeString(colTypesStr);
  
        rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(colNames, colTypes);
        rowOI = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);
    }

    @Override
    public Object deserialize(Writable blob) throws SerDeException {
        row.clear();

        String line = blob.toString();

        Pattern p = Pattern.compile("^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+)(?: \"([^\"]+)\")?(?: \"([^\"]+)\")?");
        Matcher m = p.matcher(line);

        if (m.find()) {
            for (int i = 1; i <= m.groupCount(); i++) {
                row.add(m.group(i));
            }

            String request = m.group(5);

            Map<Object, Object> map = null;
            if (!request.equals("-")) {
                String[] request_parts = request.split(" ");
                if (request_parts.length == 3) {
                    String url = request_parts[1];
                    map = new HashMap();

                    int qm = url.indexOf('?');
                    if (qm != -1) {
                        String[] kvs = url.substring(qm + 1).split("&");
                        for (String kv : kvs) {
                            String[] key_val = kv.split("=", 2);

                            if (key_val.length > 1) {
                                map.put(key_val[0], key_val[1]);
                            } else {
                                map.put(key_val[0], "");
                            }
                        }
                    }
                }
            }
            row.add(map);
        }

        return row;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return rowOI;
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return Text.class;
    }

    @Override
    public Writable serialize(Object obj, ObjectInspector oi) throws SerDeException {
        return new Text();
    }
}
