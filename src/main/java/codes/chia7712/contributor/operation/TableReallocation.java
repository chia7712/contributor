package codes.chia7712.contributor.operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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

public class TableReallocation {
  public static void main(String[] args) throws IOException {
    List<TableName> tables = Arrays.asList(args)
        .stream().map(TableName::valueOf).collect(Collectors.toList());
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
