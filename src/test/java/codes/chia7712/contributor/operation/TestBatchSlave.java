package codes.chia7712.contributor.operation;

import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class TestBatchSlave {
  
  public TestBatchSlave() {
  }
  
  @BeforeClass
  public static void setUpClass() {
  }
  
  @AfterClass
  public static void tearDownClass() {
  }
  
  @Before
  public void setUp() {
  }
  
  @After
  public void tearDown() {
  }

  @Test
  public void testCreateRow() {
    RowWork work = Mockito.mock(RowWork.class);
    Set<byte[]> rows = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    for (int i = 0; i != 10000; ++i) {
      Mockito.when(work.getRowIndex()).thenReturn((long) i);
      byte[] row = BatchSlave.createRow(work);
      assertTrue(rows.add(row));
    }
  }
  
}