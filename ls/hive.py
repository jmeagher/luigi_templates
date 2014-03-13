import luigi
import luigi.hive

compression_settings="""
    set hive.exec.compress.output=true;
    set mapred.output.compress=true;
    set mapreduce.output.fileoutputformat.compress=true;
    set io.seqfile.compression.type=BLOCK;
    set mapred.output.compression.type=BLOCK;
    set mapred.output.compression.codec={0};
    set mapreduce.map.output.compress.codec={0};
    set mapred.map.output.compression.codec={0};
""".format("org.apache.hadoop.io.compress.SnappyCodec")


class HiveDropTable(luigi.hive.HiveQueryTask):
    tablename = luigi.Parameter()
    databasename = luigi.Parameter()
    debug = luigi.BooleanParameter(False, significant=False)

    def query(self):
        q = "DROP TABLE {1}.{0}".format(self.tablename, self.databasename)
        if self.debug:
            print "  ##########################################################"
            print q
            print "  ##########################################################"
        return q

    def output(self):
        return luigi.hive.HiveTableTarget(self.tablename, database=self.databasename)

    def complete(self):
        return not super(HiveDropTable, self).complete()


def HiveTableExists(tablename, creation_query, databasename="default", debug_default=False, force_drop_default=False):
    """
    Utility template for creating a hive table if it doesn't exist yet.
    The creation string can either contain the database and table names directly
    or can use {1} and {0} to refer to them.  
    Params:
      tablename: The table that will be created
      creation_query: The SQL to create the table
      databasename: Optional database name for the table, defaults to "default"
    Usage: 
        class TableINeed(ls.hive.HiveTableExists("my_table", "create table {1}.{0} (blah string)", "my_db")):
            pass

        class OtherTableINeed(ls.hive.HiveTableExists("my_other_table", "create table {1}.{0} (foo string)")):
            pass

        class CTASExample(ls.hive.HiveTableExists("purchases_by_day", "create table {1}.{0} as select day, count(*) c from purchases group by day")):
            pass
    """

    class HiveCreateTable(luigi.hive.HiveQueryTask):
        debug = luigi.BooleanParameter(debug_default, significant=False)
        skip_compress = luigi.BooleanParameter(False, significant=False, description="Compression is enabled by default, use this to skip compression")
        force_drop = luigi.BooleanParameter(force_drop_default, significant=False, description="USE WITH CAUTION, this will drop the table and rerun")

        def query(self):
            settings = ""
            if not self.skip_compress:
                settings = compression_settings
            q = settings + "\n" + creation_query.format(tablename, databasename)
            if self.debug:
                print "  ##########################################################"
                print "table='{0}'  db='{1}'".format(tablename, databasename)
                print q
                print "  ##########################################################"
            return q

        def output(self):
            return luigi.hive.HiveTableTarget(tablename, database=databasename)

        def requires(self):
            if self.force_drop:
                return HiveDropTable(tablename=tablename, databasename=databasename, debug=self.debug)
            else:
                return super(HiveCreateTable, self).requires()

        def complete(self):
            if self.force_drop:
                return False
            else:
                return super(HiveCreateTable, self).complete()

    return HiveCreateTable




def HiveDailyPartitionedTable(tablename, daily_partition_column, creation_query, insertion_query,  databasename="default", debug_default=False):
    """
    Utility template for creating a hive table that is partitioned daily.  This covers
    initial table creation and inserting data every day.  
    Params:
      tablename: The table that will be created
      daily_partition_column: Column name for partitioning (day or something like that)
      creation_query: The SQL to create the table, see HiveTableExists docs for more info
      insertion_query: The SQL select statement to use to fill each partition.  The string {0} will be set to the day
      databasename: Optional database name for the table, defaults to "default"
    Usage: 
        class MyPartitionedTable((ls.hive.HiveDailyPartitionedTable("daily_purchases_by_type", "day", 
         "create table {0}.{1} (item_type string, purchases int, gross double, net double) partitioned by (day string) stored as TEXTFILE",
         "SELECT item_type, count(*), sum(gross_spend_tot), sum(net_spend_tot) FROM purchases WHERE purchase_day='{0}' GROUP BY item_type, purchase_day", 
         databasename="jmeagher")):
            pass
    """

    class HiveDailyTableQuery(luigi.hive.HiveQueryTask):
        date = luigi.DateParameter()
        debug = luigi.BooleanParameter(False, significant=False)
        skip_compress = luigi.BooleanParameter(False, significant=False)
        force_drop = luigi.BooleanParameter(False, significant=False, description="USE WITH CAUTION, this will drop the table and rerun")

        def requires(self):
            class MyTableExists(HiveTableExists(tablename, creation_query, databasename=databasename, debug_default=self.debug, force_drop_default=self.force_drop)):
                pass

            return [MyTableExists()]

        def output(self):
            return luigi.hive.HivePartitionTarget(tablename, {daily_partition_column:self.date.strftime('%Y-%m-%d')}, databasename)

        def query(self):
            out = self.output()
            settings = ""
            if not self.skip_compress:
                settings = compression_settings
            i = "INSERT OVERWRITE TABLE {1}.{0} PARTITION({2}) ".format(tablename, databasename, out.client.partition_spec(out.partition))
            q = insertion_query.format(self.date.strftime('%Y-%m-%d'))
            full_query = settings + "\n" + i + "\n" + q + " ;"
            if self.debug:
                print "  ##########################################################"
                print full_query
                print "  ##########################################################"
            return full_query

    class HiveDailyTableBatchQuery(luigi.Task):
        date_interval = luigi.DateIntervalParameter()
        debug = luigi.BooleanParameter(False)
        skip_compress = luigi.BooleanParameter(False)
        force_drop = luigi.BooleanParameter(False, significant=False, description="USE WITH CAUTION, this will drop the table and rerun")

        def requires(self):
            return [HiveDailyTableQuery(date=date, debug=self.debug, skip_compress=self.skip_compress, force_drop=self.force_drop) for date in self.date_interval]
        
        def complete(self):
            return False

    return HiveDailyTableBatchQuery



if __name__ == '__main__':
    luigi.run()
