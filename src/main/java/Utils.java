
import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import me.timyang.personal.company.CompanyProto;
import org.apache.commons.lang.StringUtils;
import java.util.Optional;


/**
 * Created by yt on 2017/4/18.
 */
public class Utils {
    private static final Pattern PHONE_PATTERN = Pattern.compile(".*(\\d{7}).*");    private Database companyDatabase;

    public static boolean matchEmail(String email1, String email2) {
        if (StringUtils.isEmpty(email1) || StringUtils.isEmpty(email2)) {
            return false;
        }
        return StringUtils.equals(email1, email2);
    }

    public static boolean matchPhone(String phone1, String phone2) {
        if (StringUtils.isEmpty(phone1) || StringUtils.isEmpty(phone2)) {
            return false;
        }
        Matcher matcher1 = PHONE_PATTERN.matcher(phone1);
        Matcher matcher2 = PHONE_PATTERN.matcher(phone2);
        if (matcher1.matches() && matcher2.matches()) {
            return matcher1.group(1).contains(matcher2.group(1)) || matcher2.group().contains(matcher1.group());
        }
        return false;
    }

    public static boolean matchName(String name1, String name2) {
        if (StringUtils.isEmpty(name1) || StringUtils.isEmpty(name2)) {
            return false;
        }
        return StringUtils.equals(name1, name2);
    }

    public static boolean match(CompanyProto.Item item, CompanyProto.Company company) {
        CompanyProto.Index itemIndex = item.getIndex();
        CompanyProto.Index companyIndex = company.getIndex();
        if (matchEmail(itemIndex.getEmail(), companyIndex.getEmail())) {
            return true;
        }
        if (matchPhone(itemIndex.getPhone(), companyIndex.getPhone())) {
            return true;
        }
        if (matchName(itemIndex.getName(), companyIndex.getName())) {
            return true;
        }
        return false;
    }

    public static CompanyProto.Item toItemProto(Table itemTable, Row row) {
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

    public static CompanyProto.Company toCompanyProto(Table companyTable, Row row) {
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
}
