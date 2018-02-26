/**
 * Created by Tarek on 2/19/18.
 */
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.util.ArrayList;
import java.util.List;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

public class InsertIntoKuduTable {
    public static void main(String args[])
    {
        System.out.println("Usage: KuduMasterHost:port TableName FilePath HeaderDataTypes");
        System.out.println("HeaderDataTypes must be ',' separated and has same columns order");
        System.out.println("Kudu available data types: INT32,STRING,INT64,DOUBLE");
        if(args.length<4)
        {
            System.out.println("ERROR, Number of Arguments must be greater than 4");
            System.out.println("KuduMasterHost:port TableName FilePath Header");
            return;
        }
        /*
         * Sample input arguments
         * localhost:1001 TABLE_NAME 
         */
        String KUDUMASTER   = args[0];
        String TableName    = args[1];
        String FilePath     = args[2];
        String Header       = args[3];
        String[] HeaderSplited = Header.split(",");
        System.out.println("#################### Table Specifications #############################");
        System.out.println("Kudu Master: "+KUDUMASTER);
        System.out.println("Table Name: "+TableName);
        System.out.println("File: "+FilePath);
        System.out.println("HeaderDataTypes: "+Header);
        System.out.println("#######################################################################");
        List<ColumnSchema> columns = new ArrayList(2);
        List<String> RangeKeys = new ArrayList();
        try
        {
            KuduClient client = new KuduClient.KuduClientBuilder(KUDUMASTER).build();
            KuduTable table = client.openTable(TableName);
            KuduSession session = client.newSession();
            File file = new File(FilePath);
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String [] line_splited = line.split(",");
                Insert insert = table.newInsert();
                PartialRow row = insert.getRow();
                for(int i=0;i<HeaderSplited.length;i++)
                {
                    if(HeaderSplited[i].equals("INT32"))
                    {
                        row.addInt(i,Integer.parseInt(line_splited[i]));
                    }
                    else if(HeaderSplited[i].equals("STRING"))
                    {
                        row.addString(i,line_splited[i]);
                    }
                    else if(HeaderSplited[i].equals("INT64"))
                    {
                        row.addLong(i,Long.parseLong(line_splited[i]));
                    }
                    else if(HeaderSplited[i].equals("DOUBLE"))
                    {
                        row.addDouble(i,Double.parseDouble(line_splited[i]));
                    }
                }
                System.out.println(insert.getRow().stringifyRowKey());
                session.apply(insert);
            }
            fileReader.close();
            System.out.println("Task Completed Successfully");
        }
        catch (Exception e)
        {
            System.out.println("Error due to:"+e);
        }

    }

}
