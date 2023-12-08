package opensearch.kafka;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class BonsaiConfig {
    private String connString;

    public BonsaiConfig(String filepath){
        try {
            File config_file = new File(filepath);
            FileReader config_fr = new FileReader(config_file);
            BufferedReader config_br = new BufferedReader((config_fr));
            StringBuffer config_sb = new StringBuffer();
            this.setConnString(config_br.readLine());
        } catch(IOException e){
            e.printStackTrace();
        }
    }

    public String getConnString() {
        return connString;
    }

    public void setConnString(String connString) {
        this.connString = connString;
    }
}
