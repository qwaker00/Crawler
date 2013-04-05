import java.util.ArrayList;

public class MetaStats implements Cloneable
{
    int pageCount;
    int lastTenI;
    String[] lastTen;
    ArrayList<Integer> docSizeStat;
    ArrayList<Integer> depthStat;

    public MetaStats()
    {
        this.pageCount = 0;
        this.lastTenI = 0;
        this.lastTen = new String[10];
        this.docSizeStat = new ArrayList<>();
        this.depthStat = new ArrayList<>();
    }

    public MetaStats clone()
    {
        MetaStats clone = new MetaStats();
        clone.pageCount = this.pageCount;
        clone.lastTenI = this.lastTenI;
        clone.lastTen = this.lastTen.clone();
        clone.docSizeStat = new ArrayList<>(this.docSizeStat);
        clone.depthStat = new ArrayList<>(this.depthStat);
        return clone;
    }

    public void addPage(PageData p) {
        this.pageCount += 1;
        if (this.pageCount <= 10) {
            this.lastTen[(this.pageCount - 1)] = p.url;
        } else {
            this.lastTen[this.lastTenI] = p.url;
            this.lastTenI = ((this.lastTenI + 1) % 10);
        }
        this.docSizeStat.add(p.contentSize);
        this.depthStat.add((int)p.detph);
    }
}
