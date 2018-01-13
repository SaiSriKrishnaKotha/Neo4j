package get;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import Util.Util;
import result.NodeResult;
import result.RelationshipResult;

import java.util.stream.Stream;

public class Get {

    @Context
    public GraphDatabaseService db;

    public Get(GraphDatabaseService db) {
        this.db = db;
    }

    public Get() {
    }

    @Procedure
    @Description("apoc.get.nodes(node|id|[ids]) - quickly returns all nodes with these id's")
    public Stream<NodeResult> nodes(@Name("nodes") Object ids) {
        return Util.nodeStream(db, ids).map(NodeResult::new);
    }

    @Procedure
    @Description("apoc.get.rels(rel|id|[ids]) - quickly returns all relationships with these id's")
    public Stream<RelationshipResult> rels(@Name("relationships") Object ids) {
        return Util.relsStream(db, ids).map(RelationshipResult::new);
    }

}
