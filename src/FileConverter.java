import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;

public class FileConverter {

    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("./output/part-r-00000"));
        String line = br.readLine();
        FileWriter fileWriter = new FileWriter("./output/top_cost_drug.txt");

        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

        bufferedWriter.write("drug_name,num_prescriber,total_cost");
        bufferedWriter.newLine();

        while (line != null) {
            String[] tokens = line.trim().split("\t");

            BigDecimal num = BigDecimal.valueOf(Float.parseFloat(tokens[0].trim()));
            num = num.setScale(0, RoundingMode.CEILING);

            String numString = num.toString();

            bufferedWriter.write(tokens[1].trim() + "," +
                    tokens[2].trim() + "," + numString);

            bufferedWriter.newLine();

            line = br.readLine();
        }

        br.close();
        bufferedWriter.close();
    }

}
