package ru;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunner;

import javax.sql.DataSource;
import java.sql.*;


public class CalcitePostgresMysqlJDBC {


    public static void main(String[] args) throws SQLException {
        final String dbUrl = "jdbc:postgresql://localhost:5432/people";

        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        final DataSource psDataSource = JdbcSchema.dataSource(
                "jdbc:postgresql://localhost:5432/people",
        "org.postgresql.Driver",
            "postgres",
            "p2s28");

        final DataSource mysqlDataSource = JdbcSchema.dataSource(
                "jdbc:mysql://localhost/pets",
                "com.mysql.jdbc.Driver",
                "root",
                "sH28uu9o#"
        );

        rootSchema.add("public", JdbcSchema.create(rootSchema, "public", psDataSource, null, null));
        rootSchema.add("pets", JdbcSchema.create(rootSchema, "pets", mysqlDataSource, null, null));

        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .build();

        RelBuilder rb = RelBuilder.create(config);
        RelNode node = rb
                .scan("public", "owners").as("owners")
                .scan("pets", "pets")
                .join(JoinRelType.FULL, "owner_id")
                .project(
                        rb.field("owners", "owner_id"),
                        rb.field("owners", "name"),
                        rb.field("pets", "name")
                )
                .build();

        HepProgram program = HepProgram.builder().build();
        HepPlanner planner = new HepPlanner(program);

        planner.setRoot(node);

        RelNode optimizedNode = planner.findBestExp();

        final RelShuttle shuttle = new RelHomogeneousShuttle() {
            @Override public RelNode visit(TableScan scan) {
                final RelOptTable table = scan.getTable();
                if (scan instanceof LogicalTableScan && Bindables.BindableTableScan.canHandle(table)) {
                    return Bindables.BindableTableScan.create(scan.getCluster(), table);
                }
                return super.visit(scan);
            }
        };

        optimizedNode = optimizedNode.accept(shuttle);

        final RelRunner runner = connection.unwrap(RelRunner.class);
        PreparedStatement ps = runner.prepare(optimizedNode);

        ps.execute();

        ResultSet resultSet = ps.getResultSet();
        while (resultSet.next()) {
            System.out.println(resultSet.getString("owner_id") + " " +
                                resultSet.getString(2) + " " +
                                resultSet.getString(3));
        }
    }

}
