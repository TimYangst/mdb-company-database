import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by yt on 2017/4/17.
 */
public class UtilsTest {

    @Test
    public void testMatchPhone(){
        assertTrue(Utils.matchPhone("010-65546141", "65546141"));
        assertTrue(Utils.matchPhone("010-65546141-1", "010-65546141"));
        assertTrue(Utils.matchPhone("010-65546141", "010-65546141-1"));
        assertTrue(Utils.matchPhone("010 65546141", "65546141"));
        assertTrue(Utils.matchPhone("01065546141", "010-65546141"));
    }
}
