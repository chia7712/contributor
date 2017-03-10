package codes.chia7712.contributor.operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

public class TableMa {
  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("[Usage] cmd <args>");
    }
    List<String> otherArgs = new ArrayList<>(args.length - 1);
    for (int i = 1; i != args.length; ++i) {
      otherArgs.add(args[i]);
    }
    switch (args[0].toLowerCase()) {
      case "remove":
        remove(otherArgs);
        break;
      case "group":
        group(otherArgs);
        break;
      case "help":
        System.out.println("remove <table name>");
        System.out.println("group <table name>");
        break;
      default:
        System.out.println("What are you talking??");
    }
  }
  public static void remove(List<String> args) throws IOException {
    List<TableName> tables = args.stream().map(TableName::valueOf).collect(Collectors.toList());
    if (tables.isEmpty()) {
      throw new RuntimeException("Please assign table name");
    }
    try (Connection con = ConnectionFactory.createConnection();
        Admin admin = con.getAdmin()) {
      for (TableName name : tables) {
        if (admin.tableExists(name)) {
          admin.disableTable(name);
          admin.deleteTable(name);
          System.out.println("Removd " + name);
        }
      }
    }
  }
  public static void group(List<String> args) throws IOException {
    List<TableName> tables = args.stream().map(TableName::valueOf).collect(Collectors.toList());
    if (tables.isEmpty()) {
      throw new RuntimeException("Please assign table name");
    }
    try (Connection con = ConnectionFactory.createConnection();
        Admin admin = con.getAdmin()) {
      for (TableName name : tables) {
        if (!admin.tableExists(name)) {
          throw new RuntimeException("Table:" + name + " doesn't exist");
        }
      }
      Set<ServerName> hasAssigned = new TreeSet<>();
      List<ServerName> servers = new ArrayList<>(admin.getClusterStatus().getServers());
      if (servers.size() < tables.size()) {
        throw new RuntimeException("The number of servers:" + servers.size()
          + " is less than the number of tables:" + tables.size());
      }
      for (TableName name : tables) {
        ServerName chosen;
        while (true) {
          chosen = servers.get((int) (Math.random() * servers.size()));
          if (!hasAssigned.contains(chosen)){
            break;
          }
        }
        hasAssigned.add(chosen);
        for (HRegionInfo r : admin.getTableRegions(name)) {
          admin.move(r.getEncodedNameAsBytes(), Bytes.toBytes(chosen.toString()));
        }
      }
    }
  }
}
