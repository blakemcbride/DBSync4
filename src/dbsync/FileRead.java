package dbsync;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;

/**
 *
 * @author Blake McBride
 */
public class FileRead {

	private BufferedReader port;
	private final LinkedList<Character> push_list;
	private boolean eof;

	public FileRead(String file) {
		try {
			port = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException ex) {
			ex.printStackTrace();
			System.exit(-1);
		}
		push_list = new LinkedList<Character>();
		eof = false;
	}

	public char readc() {
		if (!push_list.isEmpty()) {
			char c = push_list.getFirst();
			push_list.removeFirst();
			return c;
		} else if (eof)
			return (char) 0;
		else {
			int c = 0;
			try {
				c = port.read();
				if (c == -1) {
					eof = true;
					port.close();
					port = null;
					return (char) 0;
				}

			} catch (Exception ex) {
				ex.printStackTrace();
				System.exit(-1);
			}
			return (char) c;
		}
	}

	public char pushc(char c) {
		push_list.addFirst(c);
		return c;
	}

	public char peekc() {
		char c = this.readc();
		if (c == (char) 0)
			return c;
		else
			push_list.addFirst(c);
		return c;
	}

	public void Close() {
		if (port != null) {
			try {
				port.close();

			} catch (IOException ex) {
			}
			port = null;
		}
	}
}
