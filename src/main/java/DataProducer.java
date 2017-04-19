import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import me.timyang.personal.company.CompanyProto;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by yt on 2017/4/19.
 */
class DataProducer implements Runnable {
    private static final Logger logger = Logger.getLogger(DataProducer.class.getName());

    private BlockingQueue<CompanyProto.Item> items;
    private final Table itemTable;
    private AtomicBoolean finished;
    private AtomicInteger remains;

    public DataProducer(Table itemTable) {
        this.itemTable = itemTable;
        this.items = new ArrayBlockingQueue<CompanyProto.Item>(32768);
        this.finished = new AtomicBoolean(false);
        this.remains = new AtomicInteger(0);
    }

    @Override
    public void run() {
        for (Row itemRow : itemTable) {
            CompanyProto.Item item = Utils.toItemProto(itemTable, itemRow);
            try {
                remains.incrementAndGet();
                items.put(item);
            } catch (InterruptedException e) {
                logger.log(Level.SEVERE, "Interrupt happen in Producer: ", e);
                e.printStackTrace();
            }
        }
        finished.set(true);
    }

    public BlockingQueue<CompanyProto.Item> getItems() {
        return items;
    }

    public AtomicInteger getRemains() {
        return remains;
    }

    public boolean isFinished() {
        return finished.get();
    }
}
