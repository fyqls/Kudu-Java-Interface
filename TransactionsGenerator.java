/**
 * Created by Tarek on 2/19/18.
 */
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

public class TransactionsGenerator {
    public static double generateRandomValue()
    {
        return ThreadLocalRandom.current().nextDouble(1000,1000000);
    }
    public static int generateRandomType()
    {
        return ThreadLocalRandom.current().nextInt(1,20);
    }

    public static void main(String args[])
    {
        System.out.println("Usage: KuduMasterHost:port TableName CustomersIdsFilePath LastTransactionID NumberOfTransactionPerCustomer");
        System.out.println("Assuming Table's columns order as : transaction_id,transaction_timestamp,customer_id,value,type");
        if(args.length<5)
        {
            System.out.println("ERROR, Number of Arguments must be greater than 5");
            System.out.println("KuduMasterHost:port TableName FilePath Header");
            return;
        }
        String KUDUMASTER   = args[0];
        String TableName    = args[1];
        String FilePath     = args[2];
        int LastTransactionID  = Integer.parseInt(args[3]);
        int NumberOfTransactions = Integer.parseInt(args[4]);
        System.out.println("#################### Table Specifications #############################");
        System.out.println("Kudu Master: "+KUDUMASTER);
        System.out.println("Table Name: "+TableName);
        System.out.println("File: "+FilePath);
        System.out.println("Last Transaction ID: "+LastTransactionID);
        System.out.println("Number Of Transactions Per Customer: "+NumberOfTransactions);
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

                for(int i=0;i<=NumberOfTransactions;i++)
                {
                    Insert insert = table.newInsert();
                    PartialRow row = insert.getRow();
                    LastTransactionID = LastTransactionID + 1;
                    long timestamp = System.currentTimeMillis();
                    int customer_id = Integer.parseInt(line);
                    double value = generateRandomValue();
                    int type = generateRandomType();
                    row.addInt(0, LastTransactionID);
                    row.addLong (1, timestamp);
                    row.addInt(2, customer_id);
                    row.addDouble(3, value * 1.0);
                    row.addInt(4, type);
                    session.apply(insert);
                }
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
