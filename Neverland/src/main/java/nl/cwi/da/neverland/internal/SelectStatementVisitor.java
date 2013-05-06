package nl.cwi.da.neverland.internal;

import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;

public abstract class SelectStatementVisitor implements StatementVisitor {

	@Override
	public abstract void visit(Select select);

	@Override
	public void visit(Delete delete) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void visit(Update update) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void visit(Insert insert) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void visit(Replace replace) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void visit(Drop drop) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void visit(Truncate truncate) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void visit(CreateTable createTable) {
		throw new UnsupportedOperationException();
	}

}
