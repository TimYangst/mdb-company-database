import javafx.util.Pair;
import me.timyang.personal.company.CompanyProto;
import org.apache.commons.lang.StringUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by yt on 2017/4/19.
 */
class DataMatcher {
    private static final Logger logger = Logger.getLogger(DataMatcher.class.getName());

    private final BlockingQueue<CompanyProto.Item> items;
    private final List<CompanyProto.Company> companies;
    private final BlockingQueue<Pair<CompanyProto.Company, CompanyProto.Item>> result;
    private final AtomicInteger remains;
    private volatile Set<CompanyProto.Index> importedCompanies;
    private final AtomicBoolean finished;
    private final AtomicInteger mismatchItems;
    private final AtomicInteger mismatchCompanies;
    private final AtomicInteger processedItems;

    public DataMatcher(BlockingQueue<CompanyProto.Item> queue, List<CompanyProto.Company> companies, AtomicInteger remains) {
        this.items = queue;
        this.companies = companies;
        this.remains = remains;
        this.result = new ArrayBlockingQueue(32768);
        this.importedCompanies = new HashSet<>();
        this.finished = new AtomicBoolean(false);
        this.mismatchCompanies = new AtomicInteger(0);
        this.mismatchItems = new AtomicInteger(0);
        this.processedItems = new AtomicInteger(0);
    }

    public boolean isFinished() {
        return finished.get();
    }

    public BlockingQueue<Pair<CompanyProto.Company, CompanyProto.Item>> getResult() {
        return result;
    }

    public int getMismatchItems() {
        return mismatchItems.get();
    }

    public int getMismatchCompanies() {
        return mismatchCompanies.get();
    }

    public int getProcessedItems() {
        return processedItems.get();
    }

    public Runnable createWorker() {
        return new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        CompanyProto.Item item = items.take();
                        boolean found = false;
                        for (CompanyProto.Company company : companies) {
                            if (Utils.match(item, company)) {
                                importedCompanies.add(company.getIndex());
                                result.put(new Pair(company, item));
                                found = true;
                                if (!StringUtils.equals(company.getIndex().getName(), item.getIndex().getName())) {
                                    logger.info(String.format("Found match %s vs %s",
                                            company.getIndex().getName(), item.getIndex().getName()));
                                }
                                break;
                            }
                        }
                        if (!found) {
                            result.put(new Pair<CompanyProto.Company, CompanyProto.Item>(null, item));
                            mismatchItems.incrementAndGet();
                        }
                        remains.decrementAndGet();
                        if (processedItems.incrementAndGet() % 5000 == 0) {
                            logger.info(String.format("Processed %d records...", processedItems.get()));
                        }
                    } catch (InterruptedException e) {
                         logger.log(Level.SEVERE, "Interrupt happened at consumer.", e);
                        e.printStackTrace();
                        break;
                    }
                }
            }
        };
    }

    public Runnable getMismatchCompaniesWriter() {
        return new Runnable() {
            @Override
            public void run() {
                for (CompanyProto.Company company : companies) {
                    if (!importedCompanies.contains(company.getIndex())) {
                        try {
                            result.put(new Pair(company, null));
                            if (mismatchCompanies.incrementAndGet() % 1000 == 0) {
                                logger.info(String.format("Found %d mismatched companies", mismatchCompanies.get()));
                            }
                        } catch (InterruptedException e) {
                            logger.log(Level.SEVERE, "Interrupt happens while output mismatch companies.", e);
                            e.printStackTrace();
                        }
                    }
                }
                finished.set(true);
            }
        };
    }
}
