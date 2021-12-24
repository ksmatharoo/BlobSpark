package notused;

import java.util.ArrayList;
import java.util.List;

public class Utils {

    public enum pairState {
        unknown,
        started,
        finished,
        nonStart_Finished,
    }

    public static void main(String[] args) {

        String str = "{col1}_{col2}_{col4.jpg";
        //String  str = "${1}_${2}.jpg|1,2";

        List<String> strings = getColNameFromDerivationLogic(str, '{', '}');

        System.out.println("end");
        // write your code here
    }

    public static List<String> getColNameFromDerivationLogic(String str, char openMarker, char closeMarker) throws RuntimeException {
        int start = 0;
        int end = 0;
        Utils.pairState state = pairState.unknown;

        List<String> colName = new ArrayList<>();
        for (int i = 0; i < str.length(); i++) {
            char ch = str.charAt(i);
            if (ch == openMarker) {
                start = i;
                state = pairState.started;
            } else if (ch == closeMarker) {
                end = i;
                if (state == pairState.started) {
                    colName.add(str.substring(start + 1, end));
                } else {
                    state = pairState.nonStart_Finished;
                    throw new RuntimeException(String.format("Expected input : ${col1}_${col2}.jpg, Invalid Input received : %s," +
                            "Internal State : %s", str, state));
                }
                state = pairState.finished;
            }
        }
        if (state != pairState.finished) {
            throw new RuntimeException(String.format("Expected input : ${col1}_${col2}.jpg, Invalid Input received : %s," +
                    "Internal State : %s", str, state));
        }
        return colName;
    }
}
