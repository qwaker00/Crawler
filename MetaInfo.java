import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.tika.io.IOUtils;

public class MetaInfo
{
    String metaFilename;
    MetaStats stats;
    HashMap<Integer, PageData> pages;

    public MetaInfo(String outputFilename)
    {
        this.stats = new MetaStats();
        this.metaFilename = outputFilename;
        ObjectInputStream inp = null;
        try {
            inp = new ObjectInputStream(new GZIPInputStream(new FileInputStream(this.metaFilename)));
            Object data = inp.readObject();
            if (data == null) throw new InvalidClassException(null);
            this.pages = ((HashMap<Integer, PageData>)data);
            for (PageData p : this.pages.values())
                this.stats.addPage(p);
        }
        catch (Exception e)
        {
            this.pages = new HashMap<>();
        } finally {
            IOUtils.closeQuietly(inp);
        }
    }

    public HashMap<Integer, PageLinkData> getClosedLinks() {
        HashMap<Integer, PageLinkData> closedPages = new HashMap<>();
        Set<Integer> ids = this.pages.keySet();
        for (Integer i : ids) {
            PageData pd = this.pages.get(i);

            PageLinkData closedPD = new PageLinkData();
            closedPD.docid = pd.docid;
            closedPD.links = new ArrayList<>();
            for (Integer j : pd.links) {
                if (ids.contains(j))
                    closedPD.links.add(j);
            }
            closedPages.put(i, closedPD);
        }
        return closedPages;
    }

    public MetaStats getStats() {
        return this.stats.clone();
    }

    public synchronized void addPage(Page p, String arcName) {
        WebURL wu = p.getWebURL();
        PageData pd = new PageData();
        pd.docid = wu.getDocid();
        pd.parent = wu.getParentDocid();
        pd.detph = wu.getDepth();
        pd.url = wu.getURL();
        pd.charset = p.getContentCharset();
        pd.arcPath = arcName;
        pd.contentSize = p.getContentData().length;
        pd.links = new ArrayList<>();
        for (WebURL url : ((HtmlParseData)p.getParseData()).getOutgoingUrls()) {
            if (url.getDocid() != -1)
                pd.links.add(url.getDocid());
        }
        this.pages.put(pd.docid, pd);
        this.stats.addPage(pd);
    }

    public void Finish() {
        new File(this.metaFilename).getParentFile().mkdirs();
        ObjectOutputStream out = null;
        GZIPOutputStream gzOut = null;
        try {
            gzOut = new GZIPOutputStream(new FileOutputStream(this.metaFilename));
            out = new ObjectOutputStream(gzOut);
            out.writeObject(this.pages);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(gzOut);
            IOUtils.closeQuietly(out);
        }
    }
}
