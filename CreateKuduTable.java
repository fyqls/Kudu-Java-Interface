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

public class CreateKuduTable {
    public static Type StringToType(String input)
    {
        switch (input)
        {
            case "INT32":
                return Type.INT32;
            case "STRING":
                return Type.STRING;
            case "INT64":
                return Type.INT64;
            case "UNIXTIME_MICROS":
                return Type.UNIXTIME_MICROS;
            case "DOUBLE":
                return Type.DOUBLE;
            default:
                return Type.STRING;
        }
    }
    public static void main(String args[])
    {
        System.out.println("Usage: KuduMasterHost:port TableName Key/s FilePath Col1 Type");
        System.out.println("Kudu available data types: INT32,STRING,INT64,DOUBLE");
        System.out.println("Data File's header must be in same order of columns definition");
        System.out.println("Incase of Multiple keys, must be ',' separated");

        if(args.length<6)
        {
            System.out.println("ERROR, Number of Arguments must be greater than 6");
            System.out.println("KuduMasterHost:port TableName Key/s FilePath Col1 Type");
            return;
        }
        String KUDUMASTER  = args[0];
        String TableName   = args[1];
        String KeysInput   = args[2];
        String[] KeysSplited = KeysInput.split(",");
        List<String> Keys  = new ArrayList<>();
        System.out.println("#################### Table Specifications #############################");
        System.out.println("Kudu Master: "+KUDUMASTER);
        System.out.println("Table Name: "+TableName);
        for (int i=0 ;i<KeysSplited.length;i++)
        {
            Keys.add(KeysSplited[i]);
        }
        List<ColumnSchema> columns = new ArrayList(2);
        List<String> RangeKeys = new ArrayList<>();
        try
        {
            for(int i=3;i<args.length-1;i+=2)
            {
                String ColName  = args[i];
                String DataType = args[i+1];
                if(Keys.contains(ColName))
                {
                    System.out.println("Column Name: "+ColName+", Type: "+DataType+", Key: Yes");
                    columns.add(new ColumnSchema.ColumnSchemaBuilder(ColName, StringToType(DataType))
                            .key(true)
                            .build());
                    RangeKeys.add(ColName);
                }
                else
                {
                    System.out.println("Column Name: "+ColName+", Type:"+DataType+", Key: No");
                    columns.add(new ColumnSchema.ColumnSchemaBuilder(ColName, StringToType(DataType))
                            .build());
                }
            }
            System.out.println("#######################################################################");
            Scanner reader = new Scanner(System.in);
            System.out.println("Confirm Table Creation,y/n ?:");
            char confirm = reader.next().charAt(0);
            if(confirm=='y')
            {
                KuduClient client = new KuduClient.KuduClientBuilder(KUDUMASTER).build();
                Schema schema = new Schema(columns);
                CreateTableOptions schema_option = new CreateTableOptions();
                schema_option.setNumReplicas(1);
                schema_option.setRangePartitionColumns(RangeKeys);
                if(client.tableExists(TableName))
                {
                	client.deleteTable(TableName);
                }
                client.createTable(TableName, schema,
                        schema_option);
                System.out.println("Task Completed Successfully");
            }
            else
            {
                System.out.println("Operation Canceled");
            }
        }
        catch (Exception e)
        {
            System.out.println("Error due to:"+e);
        }
    }

}
