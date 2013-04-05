import java.io.Serializable;
import java.util.ArrayList;

public class PageData implements Serializable
{
    int docid;
    int parent;
    short detph;
    int contentSize;
    String url;
    String charset;
    String arcPath;
    ArrayList<Integer> links;
}
