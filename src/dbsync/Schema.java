package dbsync;

import java.util.LinkedList;

/**
 *
 * @author Blake McBride
 */
public class Schema extends LinkedList<ISchemaElement> {

	public static final int Postgres = 1;
	public static final int Microsoft = 2;
	private final int database;

	public Schema(int dbtype)
	{
		super();
		database = dbtype;
	}

	public int get_dbtype() {
		return database;
	}

	public Table find_table(String table) {
		for (ISchemaElement item : this)
			if (item instanceof Table && item.get_name().equalsIgnoreCase(table))
				return (Table) item;
		return null;
	}

	public ISchemaElement find(String name) {
		for (ISchemaElement item : this)
			if (item.get_name().equalsIgnoreCase(name))
				return item;
		return null;
	}
}
