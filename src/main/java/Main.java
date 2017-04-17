import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import com.healthmarketscience.jackcess.*;
import javafx.scene.control.Tab;
import me.timyang.personal.company.CompanyProto;
import org.apache.commons.lang.StringUtils;
import org.kohsuke.args4j.Option;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;

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

    @Option(name="output_file", usage = "Output file name")
    public String outputFile = "d:\\data\\result.mdb";

    public static void main(String[] args){
        try {
            new Main().process();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void outputTable(Table table, String path) throws IOException{
        File outputFile = new File(path);
        FileWriter fileWriter = new FileWriter(outputFile);
        for (Column column : table.getColumns()) {
            fileWriter.write(column.getName());
            fileWriter.write(",");
        }
        fileWriter.write("\n");
        for (Row companyRow : table) {
            for (Column column : table.getColumns()) {
                if (companyRow.get(column.getName()) != null) {
                    fileWriter.write(companyRow.get(column.getName()).toString());
                }
                fileWriter.write(",");
            }
            fileWriter.write("\n");
        }
        fileWriter.close();
    }

    private static void printTableNames(Database database) throws IOException {
        for (String tableName : database.getTableNames()) {
            System.out.print(tableName + ", ");
        }
    }

    private static List<CompanyProto.Company> readCompanies(Table companyTable) {
        List<CompanyProto.Company> result = new ArrayList<CompanyProto.Company>();
        for (Row row : companyTable) {
            result.add(toCompanyProto(companyTable, row));
        }
        return result;
    }

    private static CompanyProto.Company toCompanyProto(Table companyTable, Row row) {
        CompanyProto.Company.Builder companyBuilder = CompanyProto.Company.newBuilder();
        CompanyProto.Index.Builder indexBuilder = CompanyProto.Index.newBuilder();
        for (Column column : companyTable.getColumns()) {
            Optional<String> value = (row.get(column.getName()) == null)
                    ? Optional.<String>empty()
                    : Optional.of(String.valueOf(row.get(column.getName())));
            if (value.isPresent()) {
                switch (column.getName()) {
                    case "法人单位":
                        indexBuilder.setName(value.get());
                        break;
                    case "地址":
                        indexBuilder.setAddress(value.get());
                        break;
                    case "电话号码":
                        indexBuilder.setPhone(value.get());
                        break;
                    case "邮件地址":
                        indexBuilder.setEmail(value.get());
                        break;
                    case "法人":
                        indexBuilder.setPerson(value.get());
                        break;
                }
                companyBuilder.addFields(CompanyProto.Field.newBuilder()
                        .setKey(column.getName()).setValue(value.get()));
            }
        }
        companyBuilder.setIndex(indexBuilder);
        return companyBuilder.build();
    }

    private static List<CompanyProto.Item> readItems(Table itemTable) {
        List<CompanyProto.Item> result = new ArrayList<CompanyProto.Item>();
        for (Row row : itemTable) {
            result.add( toItemProto(itemTable, row));
        }
        return result;
    }

    private static CompanyProto.Item toItemProto(Table itemTable, Row row) {
        CompanyProto.Item.Builder itemBuilder = CompanyProto.Item.newBuilder();
        CompanyProto.Index.Builder indexBuilder = CompanyProto.Index.newBuilder();
        for (Column column : itemTable.getColumns()) {
            Optional<String> value = (row.get(column.getName()) == null)
                    ? Optional.<String>empty()
                    : Optional.of(String.valueOf(row.get(column.getName())));
            if (value.isPresent()) {
                switch (column.getName()) {
                    case "经营单位":
                        indexBuilder.setName(value.get());
                        break;
                    case "单位地址":
                        indexBuilder.setAddress(value.get());
                        break;
                    case "电话":
                        indexBuilder.setPhone(value.get());
                        break;
                    case "电子邮件":
                        indexBuilder.setEmail(value.get());
                        break;
                    case "联系人":
                        indexBuilder.setPerson(value.get());
                        break;
                }
                itemBuilder.addFields(CompanyProto.Field.newBuilder()
                        .setKey(column.getName()).setValue(value.get()));
            }
        }
        itemBuilder.setIndex(indexBuilder);
        return itemBuilder.build();
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

    private boolean matchEmail(String email1, String email2) {
        if (StringUtils.isEmpty(email1) || StringUtils.isEmpty(email2)) {
            return false;
        }
        return StringUtils.equals(email1, email2);
    }
    private boolean matchPhone(String phone1, String phone2) {
        if (StringUtils.isEmpty(phone1) || StringUtils.isEmpty(phone2)) {
            return false;
        }
        Pattern pattern = Pattern.compile("(\\d7\\d?)");
        Matcher matcher1 = pattern.matcher(phone1);
        Matcher matcher2 = pattern.matcher(phone2);
        if (matcher1.matches() && matcher2.matches()) {
            return matcher1.group().contains(matcher2.group()) || matcher2.group().contains(matcher1.group());
        }
        return false;
    }

    private boolean matchName(String name1, String name2) {
        if (StringUtils.isEmpty(name1) || StringUtils.isEmpty(name2)) {
            return false;
        }
        return StringUtils.equals(name1, name2);
    }

    private boolean match(CompanyProto.Item item, CompanyProto.Company company) {
        CompanyProto.Index itemIndex = item.getIndex();
        CompanyProto.Index companyIndex = company.getIndex();
        if (matchEmail(itemIndex.getEmail(), companyIndex.getEmail())) {
            return true;
        }
        if (matchPhone(itemIndex.getPhone(), companyIndex.getPhone())) {
            return true;
        }
//        if (matchPerson(itemIndex.getPerson()), companyIndex.getPerson()) {
//            return true;
//        }
        if (matchName(itemIndex.getName(), companyIndex.getName())) {
            return true;
        }
        return false;
    }

    private static void outputRow(CompanyProto.Company company, CompanyProto.Item item, Table table) throws IOException {
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
        table.addRowFromMap(dataMap);
    }


    public void process() throws IOException {
        Database companyDatabase = null;
        Database itemDatabase = null;
        Database outputDatabase = DatabaseBuilder.create(Database.FileFormat.V2003, new File(outputFile));
        try {
            companyDatabase = DatabaseBuilder.open(new File(companyFile));
            itemDatabase =  DatabaseBuilder.open(new File(itemFile));
            Table companyTable = companyDatabase.getTable(companyTableName);
            Table itemTable = itemDatabase.getTable(itemTableName);
            Table resultTable = initialTable(companyTable, itemTable, outputDatabase);
            List<CompanyProto.Company> companies = readCompanies(companyTable);
            Set<CompanyProto.Index> importedCompanies = new HashSet<>();
            for (Row itemRow : itemTable) {
                boolean found = false;
                CompanyProto.Item item = toItemProto(itemTable, itemRow);
                for (CompanyProto.Company company : companies) {
                    if (match(item, company)) {
                        importedCompanies.add(company.getIndex());
                        outputRow(company, item, resultTable);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    outputRow(null, item, resultTable);
                }
            }
            for (CompanyProto.Company company : companies) {
                if (!importedCompanies.contains(company.getIndex())) {
                    outputRow(company, null, resultTable);
                }
            }
        } catch (IOException | SQLException e) {
            e.printStackTrace();
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
