import com.healthmarketscience.jackcess.Table;
import com.sun.xml.internal.txw2.output.DataWriter;
import javafx.util.Pair;
import me.timyang.personal.company.CompanyProto;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by yt on 2017/4/18.
 */
class DatabaseWriter implements Runnable {
    private final static Logger logger = Logger.getLogger(DataWriter.class.getName());

    private final BlockingQueue<Pair<CompanyProto.Company, CompanyProto.Item>> result;
    private final Table resultTable;
    private final AtomicInteger outputCount;

    public DatabaseWriter(Table resultTable, BlockingQueue<Pair<CompanyProto.Company, CompanyProto.Item>> result) {
        this.result = result;
        this.resultTable = resultTable;
        this.outputCount = new AtomicInteger(0);
    }

    public int getOutputCount() {
        return outputCount.get();
    }

    @Override
    public void run() {
        while (true) {
            try {
                Pair<CompanyProto.Company, CompanyProto.Item> resultPair = result.take();
                outputRow(resultPair.getKey(), resultPair.getValue());
                if (outputCount.incrementAndGet() % 500 == 0) {
                    logger.info(String.format("Output %d lines.", outputCount.get()));
                }
            } catch (InterruptedException | IOException e1) {
                logger.log(Level.SEVERE, "Exception at Database Writer.", e1);
                e1.printStackTrace();
                break;
            }
        }
    }

    private void outputRow(CompanyProto.Company company, CompanyProto.Item item) throws IOException {
        Map<String, Object> dataMap = new HashMap<>();
        if (company != null) {
            for (CompanyProto.Field field : company.getFieldsList()) {
                dataMap.put(field.getKey(), field.getValue());
            }
        }
        if (item != null) {
            for (CompanyProto.Field field : item.getFieldsList()) {
                dataMap.put(field.getKey(), field.getValue());
            }
        }
        resultTable.addRowFromMap(dataMap);
    }
}
