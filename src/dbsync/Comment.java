
package dbsync;

/**
 *
 * @author Blake McBride
 */
public class Comment implements ISchemaElement {

	private String type;
	private final String what;
	private final String on;
	private final String comment;

	public Comment(String typ, String wht, String ona, String com) {
		type = typ;
		what = Main.remove_public(wht);
		on = Main.remove_public(ona);
		comment = com;
	}

	public void set_type(String typ) {
		type = typ;
	}

	public String get_type() {
		return type;
	}

	@Override
	public String get_name() {
		return type + what + on;
	}

	@Override
	public void add_postgres() {
		if (type.equals("CONSTRAINT"))
			System.out.println("COMMENT ON " + type + " \"" + what + "\" ON \"" + on + "\" IS " + comment + ";\n");
		else
			System.out.println("COMMENT ON " + type + " " + dbsync.Main.quote_two_names(what) + " IS " + comment + ";\n");
	}

	@Override
	public void add_microsoft() {
	}

	@Override
	public void add_oracle() {
		if (!type.equals("CONSTRAINT") && !type.equals("SCHEMA"))
			System.out.println("COMMENT ON " + type + " " + dbsync.Main.quote_two_names(what).toUpperCase() + " IS " + comment + ";\n");
	}

	@Override
	public void drop_postgres() {
	}

	@Override
	public void drop_microsoft() {
	}
}
