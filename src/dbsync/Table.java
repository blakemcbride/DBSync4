package dbsync;

import java.util.LinkedList;

/**
 *
 * @author Blake McBride
 */
public class Table implements ISchemaElement {

	private final String name;
	private final LinkedList<ISchemaElement> columns;

	public Table(String nam) {
		name = Main.remove_public(nam);
		columns = new LinkedList<ISchemaElement>();
	}

	public void add_column(ISchemaElement col) {
		columns.addLast(col);
	}

	@Override
	public String get_name() {
		return name;
	}

	public LinkedList<ISchemaElement> get_columns() {
		return columns;
	}

	public ISchemaElement find(String name) {
		for (ISchemaElement col : columns)
			if (col.get_name().equalsIgnoreCase(name))
				return col;
		return null;
	}

	@Override
	public void add_postgres() {
		boolean comma = false;
		System.out.println("CREATE TABLE \"" + name + "\" (");
		for (ISchemaElement c : columns) {
			if (comma)
				System.out.println(",");
			else
				comma = true;
			System.out.print("\t");
			if (c instanceof Column)
				((Column) c).add_inside_postgres();
			else if (c instanceof Check)
				((Check) c).add_inside_postgres();
		}
		System.out.println("\n);\n");
	}

	@Override
	public void add_microsoft() {
		boolean comma = false;
		System.out.println("CREATE TABLE [dbo].[" + name + "] (");
		for (ISchemaElement c : columns) {
			if (comma)
				System.out.println(",");
			else
				comma = true;
			System.out.print("\t");
			if (c instanceof Column)
				((Column) c).add_inside_microsoft();
			else if (c instanceof Check)
				((Check) c).add_inside_microsoft();
		}
		System.out.println("\n)\nGO\n");
	}

	@Override
	public void add_oracle() {
		boolean comma = false;
		System.out.println("CREATE TABLE \"" + name.toUpperCase() + "\" (");
		for (ISchemaElement c : columns) {
			if (comma)
				System.out.println(",");
			else
				comma = true;
			System.out.print("\t");
			if (c instanceof Column)
				((Column) c).add_inside_oracle();
			else if (c instanceof Check)
				((Check) c).add_inside_oracle();
		}
		System.out.println("\n);\n");
	}

	@Override
	public void drop_postgres() {
		System.out.println("DROP TABLE \"" + name + "\";\n");
	}

	@Override
	public void drop_microsoft() {
		System.out.println("DROP TABLE [dbo].[" + name + "]\nGO\n");
	}
}
