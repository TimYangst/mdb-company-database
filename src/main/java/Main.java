import com.healthmarketscience.jackcess.*;
import me.timyang.personal.company.CompanyProto;
import org.kohsuke.args4j.Option;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by yt on 2017/4/9.
 */


public class Main {
    private static Logger logger = Logger.getLogger(Main.class.getName());

    @Option(name = "company", usage = "Company db file name")
    public String companyFile = "d:\\data\\2003.mdb";

    @Option(name = "company_table", usage = "Company table name")
    public String companyTableName = "qy03";

    @Option(name = "item", usage = "Item db file name")
    public String itemFile = "d:\\data\\data200301.mdb";

    @Option(name = "item_table", usage = "Item table name")
    public String itemTableName = "data200301";

    @Option(name = "output_file", usage = "Output file name")
    public String outputFile = "d:\\data\\result.mdb";

    private Database itemDatabase;
    private Database outputDatabase;
    private Database companyDatabase;

    public static void main(String[] args) {
        try {
            new Main().process();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static List<CompanyProto.Company> readCompanies(Table companyTable) {
        List<CompanyProto.Company> result = new ArrayList<CompanyProto.Company>();
        int count = 0;
        for (Row row : companyTable) {
            result.add(Utils.toCompanyProto(companyTable, row));
            count++;
            if (count % 100 == 0) {
                logger.info(String.format("Parsed %d companies.", count));
            }
        }
        logger.info(String.format("Total parsed %d companies", count));
        return result;
    }

    private static Table initialTable(Table companyTable, Table itemTable, Database outputDatabase) throws IOException, SQLException {
        TableBuilder resultTable = new TableBuilder("result");
        for (Column column : companyTable.getColumns()) {
            resultTable.addColumn(new ColumnBuilder(column.getName()).setSQLType(Types.CHAR));
        }
        for (Column column : itemTable.getColumns()) {
            resultTable.addColumn(new ColumnBuilder(column.getName()).setSQLType(Types.CHAR));
        }
        return resultTable.toTable(outputDatabase);
    }




    private void initialize() throws IOException {
        logger.info("Start program...");
        logger.info(String.format("Company database: %s; Record database: %s", companyFile, itemFile));
        logger.info(String.format("Output path: %s", outputFile));
        logger.info("Connecting to database...");
        companyDatabase = DatabaseBuilder.open(new File(companyFile));
        itemDatabase = DatabaseBuilder.open(new File(itemFile));
        outputDatabase = DatabaseBuilder.create(Database.FileFormat.V2003, new File(outputFile));
    }

    public void process() throws IOException {
        try {
            try {
                initialize();
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Initialize database failed.", e);
                e.printStackTrace();
                return;
            }
            logger.info("Finished init databases.");
            Table companyTable = null;
            Table itemTable = null;
            Table resultTable = null;
            try {
                companyTable = companyDatabase.getTable(companyTableName);
                itemTable = itemDatabase.getTable(itemTableName);
                logger.info("Start initialize output database....");
                resultTable = initialTable(companyTable, itemTable, outputDatabase);
            } catch (IOException | SQLException e) {
                logger.log(Level.SEVERE, "Initialize tables failed.", e);
                e.printStackTrace();
                return;
            }
            logger.info( "Start reading companies output database....");
            List<CompanyProto.Company> companies = readCompanies(companyTable);

            ExecutorService executor = Executors.newCachedThreadPool();

            DataProducer dataProducer = new DataProducer(companyTable);
            DataMatcher matcher = new DataMatcher(dataProducer.getItems(), companies, dataProducer.getRemains());
            DatabaseWriter writer = new DatabaseWriter(resultTable, matcher.getResult());

            executor.execute(dataProducer);
            for (int i = 0; i < 64; i++) {
                executor.execute(matcher.createWorker());
            }
            executor.execute(writer);

            while (true) {
                try {
                    Thread.sleep(5000);
                    if (dataProducer.isFinished() && dataProducer.getRemains().get() == 0) {
                        break;
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }
            }
            logger.info("All records are processed to match, output mismatch companies...");
            executor.execute(matcher.getMismatchCompaniesWriter());
            while (true) {
                try {
                    Thread.sleep(5000);
                    if (matcher.isFinished() && matcher.getResult().isEmpty()) {
                        Thread.sleep(500);
                        break;
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            logger.info("All records are written....");
            logger.info( String.format(
                    "Total parse records: %d, mismatch record: %d, mismatch company: %d",
                    matcher.getProcessedItems(), matcher.getMismatchItems(), matcher.getMismatchCompanies()));
            logger.info(String.format("Output %d rows", writer.getOutputCount()));
        } finally {
            if (companyDatabase != null) {
                companyDatabase.close();
            }
            if (itemDatabase != null) {
                itemDatabase.close();
            }
            if (outputDatabase != null) {
                outputDatabase.close();
            }
        }

    }
}
