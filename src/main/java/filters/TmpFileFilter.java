import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class TmpFileFilter implements PathFilter {
    public boolean accept(Path p) {
	return !p.getName().endsWith(".tmp");
    }
}
