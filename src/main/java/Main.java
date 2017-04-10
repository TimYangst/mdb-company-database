import com.healthmarketscience.jackcess.*;
import org.kohsuke.args4j.Option;

import java.io.File;
import java.io.IOException;

/**
 * Created by yt on 2017/4/9.
 */


public class Main {

    @Option(name="company", usage="Company db file name")
    public String companyFile = "d:\\data\\2003.mdb";

    @Option(name="company_table", usage="Company table name")
    public String companyTableName = "qy03";

    @Option(name="item", usage="Item db file name")
    public String itemFile = "d:\\data\\data200301.mdb";

    @Option(name="item_table", usage="Item table name")
    public String itemTableName = "data200301";

    public static void main(String[] args){
        try {
            new Main().process();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void process() throws IOException {
        Database companyDatabase = null;
        Database itemDatabase = null;
        try {
            Table companyTable = DatabaseBuilder.open(new File(companyFile)).getTable(companyTableName);
            Table itemTable = DatabaseBuilder.open(new File(itemFile)).getTable(itemTableName);
            for (Row companyRow : companyTable) {
                for (Column column : companyTable.getColumns()) {
                    System.out.print(column.getName()+ ": " + companyRow.get(column.getName()) + ", ");
                }
                System.out.print("\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (companyDatabase != null) {
                companyDatabase.close();
            }
            if (itemDatabase != null) {
                itemDatabase.close();
            }
        }

    }
}
