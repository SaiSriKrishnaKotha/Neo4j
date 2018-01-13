package create;

import org.neo4j.procedure.*;

import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Iterables;

import Util.Util;
import get.Get;
import result.NodeResult;
import result.RelationshipResult;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.graphdb.RelationshipType.withName;

public class Create {

    public static final String[] EMPTY_ARRAY = new String[0];
    @Context
    public GraphDatabaseService db;

    @Procedure(mode = Mode.WRITE)
    @Description("create.node(['Label'], {key:value,...}) - create node with dynamic labels")
    public Stream<NodeResult> node(@Name("label") List<String> labelNames, @Name("props") Map<String, Object> props) {
        return Stream.of(new NodeResult(setProperties(db.createNode(Util.labels(labelNames)),props)));
    }


    @Procedure(mode = Mode.WRITE)
    @Description("create.addLabels( [node,id,ids,nodes], ['Label',...]) - adds the given labels to the node or nodes")
    public Stream<NodeResult> addLabels(@Name("nodes") Object nodes, @Name("label") List<String> labelNames) {
        Label[] labels = Util.labels(labelNames);
        return new Get(db).nodes(nodes).map((r) -> {
            Node node = r.node;
            for (Label label : labels) {
                node.addLabel(label);
            }
            return r;
        });
    }

    @Procedure(mode = Mode.WRITE)
    @Description("create.setProperty( [node,id,ids,nodes], key, value) - sets the given property on the node(s)")
    public Stream<NodeResult> setProperty(@Name("nodes") Object nodes, @Name("key") String key, @Name("value") Object value) {
        return new Get(db).nodes(nodes).map((r) -> {
            r.node.setProperty(key,toPropertyValue(value));
            return r;
        });
    }
    @Procedure(mode = Mode.WRITE)
    @Description("create.setRelProperty( [rel,id,ids,rels], key, value) - sets the given property on the relationship(s)")
    public Stream<RelationshipResult> setRelProperty(@Name("relationships") Object rels, @Name("key") String key, @Name("value") Object value) {
        return new Get(db).rels(rels).map((r) -> {
            r.rel.setProperty(key,toPropertyValue(value));
            return r;
        });
    }

    @Procedure(mode = Mode.WRITE)
    @Description("create.setProperties( [node,id,ids,nodes], [keys], [values]) - sets the given property on the nodes(s)")
    public Stream<NodeResult> setProperties(@Name("nodes") Object nodes, @Name("keys") List<String> keys, @Name("values") List<Object> values) {
        return new Get(db).nodes(nodes).map((r) -> {
            setProperties(r.node, Util.mapFromLists(keys,values));
            return r;
        });
    }

    @Procedure(mode = Mode.WRITE)
    @Description("create.setRelProperties( [rel,id,ids,rels], [keys], [values]) - sets the given property on the relationship(s)")
    public Stream<RelationshipResult> setRelProperties(@Name("rels") Object rels, @Name("keys") List<String> keys, @Name("values") List<Object> values) {
        return new Get(db).rels(rels).map((r) -> {
            setProperties(r.rel, Util.mapFromLists(keys,values));
            return r;
        });
    }

    @Procedure(mode = Mode.WRITE)
    @Description("create.setLabels( [node,id,ids,nodes], ['Label',...]) - sets the given labels, non matching labels are removed on the node or nodes")
    public Stream<NodeResult> setLabels(@Name("nodes") Object nodes, @Name("label") List<String> labelNames) {
        Label[] labels = Util.labels(labelNames);
        return new Get(db).nodes(nodes).map((r) -> {
            Node node = r.node;
            for (Label label : node.getLabels()) {
                if (labelNames.contains(label.name())) continue;
                node.removeLabel(label);
            }
            for (Label label : labels) {
                if (node.hasLabel(label)) continue;
                node.addLabel(label);
            }
            return r;
        });
    }

    @Procedure(mode = Mode.WRITE)
    @Description("create.removeLabels( [node,id,ids,nodes], ['Label',...]) - removes the given labels from the node or nodes")
    public Stream<NodeResult> removeLabels(@Name("nodes") Object nodes, @Name("label") List<String> labelNames) {
        Label[] labels = Util.labels(labelNames);
        return new Get(db).nodes(nodes).map((r) -> {
            Node node = r.node;
            for (Label label : labels) {
                node.removeLabel(label);
            }
            return r;
        });
    }
    
    private <T extends PropertyContainer> T setProperties(T pc, Map<String, Object> p) {
        if (p == null) return pc;
        for (Map.Entry<String, Object> entry : p.entrySet()) pc.setProperty(entry.getKey(), toPropertyValue(entry.getValue()));
        return pc;
    }
    
    private Object toPropertyValue(Object value) {
        if (value instanceof Iterable) {
            Iterable it = (Iterable) value;
            Object first = Iterables.firstOrNull(it);
            if (first==null) return EMPTY_ARRAY;
            return Iterables.asArray(first.getClass(), it);
        }
        return value;
    }
}