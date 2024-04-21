package dbsync;


import java.util.LinkedList;

/**
 *
 * @author Blake McBride
 */
public class ForeignKey implements ISchemaElement {

	private final String table;
	private final String iname;
	private boolean only = false;
	private final LinkedList<String> fields;
	private final String table2;
	private final LinkedList<String> fields2;

	public ForeignKey(String nam, String tab, boolean on, LinkedList<String> flds, String tab2, LinkedList<String> flds2) {
		iname = nam;
		table = Main.remove_public(tab);
		only = on;
		fields = flds;
		table2 = Main.remove_public(tab2);
		fields2 = flds2;
	}

	@Override
	public String get_name() {
		return iname;
	}

	@Override
	public void add_postgres() {
		boolean comma = false;
		System.out.print("ALTER TABLE ONLY \"" + table + "\" ADD CONSTRAINT \"" + iname + "\" FOREIGN KEY (");
		for (String fld : fields) {
			if (comma)
				System.out.print(", ");
			else
				comma = true;
			System.out.print("\"" + fld + "\"");
		}
		System.out.print(") REFERENCES \"" + table2 + "\" (");
		comma = false;
		for (String fld : fields2) {
			if (comma)
				System.out.print(", ");
			else
				comma = true;
			System.out.print("\"" + fld + "\"");
		}
		System.out.println(");\n");
	}

	@Override
	public void add_microsoft() {
		boolean comma = false;
		System.out.print("ALTER TABLE [dbo].[" + table + "] ADD CONSTRAINT [" + iname + "] FOREIGN KEY (");
		for (String fld : fields) {
			if (comma)
				System.out.print(", ");
			else
				comma = true;
			System.out.print("[" + fld + "]");
		}
		System.out.print(") REFERENCES [dbo].[" + table2 + "] (");
		comma = false;
		for (String fld : fields2) {
			if (comma)
				System.out.print(", ");
			else
				comma = true;
			System.out.print("[" + fld + "]");
		}
		System.out.println(")\nGO\n");
	}

	@Override
	public void add_oracle() {
		boolean comma = false;
		System.out.print("ALTER TABLE \"" + table.toUpperCase() + "\" ADD CONSTRAINT \"" + dbsync.Main.limit_name(30, iname.toUpperCase()) + "\" FOREIGN KEY (");
		for (String fld : fields) {
			if (comma)
				System.out.print(", ");
			else
				comma = true;
			System.out.print("\"" + fld.toUpperCase() + "\"");
		}
		System.out.print(") REFERENCES \"" + table2.toUpperCase() + "\" (");
		comma = false;
		for (String fld : fields2) {
			if (comma)
				System.out.print(", ");
			else
				comma = true;
			System.out.print("\"" + fld.toUpperCase() + "\"");
		}
		System.out.println(");\n");
	}

	@Override
	public void drop_postgres() {
		System.out.println("ALTER TABLE \"" + table + "\" DROP CONSTRAINT \"" + iname + "\";\n");
	}

	@Override
	public void drop_microsoft() {
		System.out.println("ALTER TABLE [dbo].[" + table + "] DROP CONSTRAINT [" + iname + "]\nGO\n");
	}
}
