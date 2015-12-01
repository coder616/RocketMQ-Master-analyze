import java.util.Map;


/**
 * Created by root on 2015/9/10.
 */
public class EnvDemo {
    public static void main(String[] args) {
        Map<String, String> map = System.getenv();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            System.out.println(entry.getKey());
            System.out.println(entry.getValue());
            System.out.println("************");
        }
    }
}
