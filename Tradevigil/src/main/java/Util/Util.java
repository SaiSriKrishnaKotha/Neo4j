package Util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class Util {
	
    public static final Label[] NO_LABELS = new Label[0];

    public static Stream<Object> stream(Object values) {
        if (values == null) return Stream.empty();
        if (values instanceof Collection) return ((Collection)values).stream();
        if (values instanceof Object[]) return Stream.of(((Object[])values));
        if (values instanceof Iterable) {
            Spliterator<Object> spliterator = ((Iterable) values).spliterator();
            return StreamSupport.stream(spliterator,false);
        }
        return Stream.of(values);
    }
	
    public static Map<String, Object> merge(Map<String, Object> first, Map<String, Object> second) {
	        if (second == null || second.isEmpty()) return first == null ? Collections.EMPTY_MAP : first;
	        if (first == null || first.isEmpty()) return second == null ? Collections.EMPTY_MAP : second;
	        Map<String,Object> combined = new HashMap<>(first);
	        combined.putAll(second);
	        return combined;
	    }
	 
    public static Label[] labels(Object labelNames) {
	        if (labelNames==null) return NO_LABELS;
	        if (labelNames instanceof List) {
	            List names = (List) labelNames;
	            Label[] labels = new Label[names.size()];
	            int i = 0;
	            for (Object l : names) {
	                if (l==null) continue;
	                labels[i++] = Label.label(l.toString());
	            }
	            if (i <= labels.length) return Arrays.copyOf(labels,i);
	            return labels;
	        }
	        return new Label[]{Label.label(labelNames.toString())};
	    }
	 
	 	public static Stream<Node> nodeStream(GraphDatabaseService db, Object ids) {
	        return stream(ids).map(id -> node(db, id));
	    }

	    public static Node node(GraphDatabaseService db, Object id) {
	        if (id instanceof Node) return (Node)id;
	        if (id instanceof Number) return db.getNodeById(((Number)id).longValue());
	        throw new RuntimeException("Can't convert "+id.getClass()+" to a Node");
	    }

	    public static Stream<Relationship> relsStream(GraphDatabaseService db, Object ids) {
	        return stream(ids).map(id -> relationship(db, id));
	    }

	    public static Relationship relationship(GraphDatabaseService db, Object id) {
	        if (id instanceof Relationship) return (Relationship)id;
	        if (id instanceof Number) return db.getRelationshipById(((Number)id).longValue());
	        throw new RuntimeException("Can't convert "+id.getClass()+" to a Relationship");
	    }

	    public static Map<String, Object> mapFromLists(List<String> keys, List<Object> values) {
	        if (keys == null || values == null || keys.size() != values.size())
	            throw new RuntimeException("keys and values lists have to be not null and of same size");
	        if (keys.isEmpty()) return Collections.<String,Object>emptyMap();
	        if (keys.size()==1) return Collections.singletonMap(keys.get(0),values.get(0));
	        ListIterator<Object> it = values.listIterator();
	        Map<String, Object> res = new LinkedHashMap<>(keys.size());
	        for (String key : keys) {
	            res.put(key,it.next());
	        }
	        return res;
	    }
	    
}
