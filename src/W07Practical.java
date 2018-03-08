import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

public class W07Practical {
    String dbFileName;
    String dbURL;

    public static void main(String args[]) {
        String dbName = args[0];
        String action = args[1];
        File csvFile = new File(args[2]);

        createDatabase(dbName);

        /*if (action == "create") {
            try (Scanner inputStream = new Scanner(csvFile)) {
                int i = 0;
                while (inputStream.hasNext()) {
                    String data = inputStream.nextLine();
                    if (i == 0) {
                        String[] dataRecord = data.split(",");
                    }
                }
            } catch (FileNotFoundException e) {

            }
        }*/
    }

    public static void createDatabase(String dbName) {
        File dir = new File("src/database");
        dir.mkdirs();
        File database = new File(dir,dbName);
        try {
            database.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
