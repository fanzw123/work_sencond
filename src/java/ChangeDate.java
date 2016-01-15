import org.apache.hadoop.io.Text;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
/**
 * Created by Administrator on 2015/12/29.
 */
public class ChangeDate {
    private Text result = new Text();
    public Text evaluate(Text str, String stripChars) {
        if (str == null||stripChars ==null) {
            return null;
        }
        try {
            SimpleDateFormat sdf=new SimpleDateFormat(stripChars);
            Date date  = sdf.parse(str.toString());
            String c=sdf.format(date);
            result.set(c);
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return result;
    }
}
