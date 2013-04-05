import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.url.WebURL;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.Logger;
import org.apache.tika.io.IOUtils;

public class SimpleWikiCrawler extends WebCrawler
{
    static final private Logger logger = Logger.getLogger(SimpleWikiCrawler.class);

    private static final Pattern GoodPattern = Pattern.compile("^http://simple.wikipedia.org/wiki/[^./\\:]+$");
    private MetaInfo meta;
    private String storagePath;

    public boolean shouldVisit(WebURL url)
    {
        String href = url.getURL().toLowerCase();
        return GoodPattern.matcher(href).matches();
    }

    public void visit(Page page)
    {
        try {
            String url = page.getWebURL().getURL();
            logger.info(String.format("URL: %s [%d]", url, page.getWebURL().getDepth()));

            GZIPOutputStream out = null;
            String arcName = this.storagePath + "/arc/" + page.getWebURL().getPath().replace(":", "%3a").replace("*", "%22") + ".gz";
            try {
                new File(arcName).getParentFile().mkdirs();
                out = new GZIPOutputStream(new FileOutputStream(new File(arcName)));
                out.write(page.getContentData());
            } catch (IOException e) {
                e.printStackTrace();
            }
            finally {
                IOUtils.closeQuietly(out);
            }

            if ((this.meta != null) && (page.getWebURL().getDepth() > 0))
                this.meta.addPage(page, arcName);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void onBeforeExit()
    {
        logger.debug(getMyId() + " finishing.");
    }

    public void onStart()
    {
        logger.debug(getMyId() + " starting.");

        CrawlerParams params = (CrawlerParams)getMyController().getCustomData();
        this.storagePath = params.storagePath;
        this.meta = params.meta;
    }
}
