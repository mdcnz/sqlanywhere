package sqlanywhere

type result struct {
	stmt *statement
	con  *connection
}

func (r *result) connection() *connection {
	if r.stmt != nil && r.stmt.con != nil {
		return r.stmt.con
	}

	if r.con != nil {
		return r.con
	}

	return nil
}

func (r *result) LastInsertId() (int64, error) {
	return r.connection().queryInt("select @@identity")
}

func (r *result) RowsAffected() (int64, error) {
	return r.connection().queryInt("select @@rowcount")
}
