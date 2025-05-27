import traceback
from logging import Logger

import pymysql.cursors

from _config import (
    MYSQL_DATABASE,
    MYSQL_HOST,
    MYSQL_PASSWORD,
    MYSQL_PORT,
    MYSQL_USER,
    base_logger,
)


class MySqlNoConnectionError(Exception):
    def __init__(self):
        super().__init__("No connection to database yet.")


class MySqlNoValueInsertionError(Exception):
    def __init__(self):
        super().__init__("No value given to insert.")


class MySqlDuplicateColumnUpdateError(Exception):
    def __init__(self, column: str):
        super().__init__(f"Updating multiple time the same column, {column=}")


class MySqlNoUpdateValuesError(Exception):
    def __init__(self):
        super().__init__("Nothing given to update.")


class MySqlWrongQueryError(Exception):
    def __init__(self, detail: str | None = None) -> None:
        super().__init__(detail)


class MysqlClient:
    def __init__(self, logger: Logger | None = None):
        self.logger = logger if logger else base_logger
        self.connection: pymysql.Connection[pymysql.cursors.DictCursor] | None = None
        self.port = MYSQL_PORT
        self.host = MYSQL_HOST
        self.user = MYSQL_USER
        self.password = MYSQL_PASSWORD
        self.database = MYSQL_DATABASE
        self.__connect()

    def __connect(self):
        self.connection = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.password,
            database=self.database,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
        )

    def check_alive(self):
        try:
            try:
                check_alive_res = self.execute("select 1;")
            except:
                check_alive_res = None
            if not check_alive_res:
                self.__connect()
        except:
            self.logger.critical("ERROR: Lost connection to Database.")
            raise MySqlNoConnectionError()

    def logging(self, cursor):
        self.logger.debug(f"MysqlClient executed: {str(cursor._executed)}")
        self.logger.debug(f"{cursor.rowcount=}")

    def obj_to_str(self, o) -> str:
        if isinstance(o, int):
            return str(o)
        return f"'{o}'"

    def ls_obj_to_str(self, ls: list) -> list:
        if not ls:
            return ls
        return [self.obj_to_str(e) for e in ls]

    def generate_cond(
        self,
        cond_null: list[str] = list(),
        cond_not_null: list[str] = list(),
        cond_in: dict[str, list] = dict(),
        cond_eq: dict[str, object] = dict(),
        cond_neq: dict[str, object] = dict(),
        cond_leq: dict[str, object] = dict(),
        cond_geq: dict[str, object] = dict(),
        cond_l: dict[str, object] = dict(),
        cond_g: dict[str, object] = dict(),
    ) -> str:
        cond = " WHERE 1 = 1 "
        for col in cond_null:
            cond = cond + f" AND {col} IS NULL "
        for col in cond_not_null:
            cond = cond + f" AND {col} IS NOT NULL "
        for col, ls_val in cond_in.items():
            if not ls_val:
                continue
            cond = cond + f" AND {col} IN ({', '.join(self.ls_obj_to_str(ls_val))})"
        for col, val in cond_eq.items():
            if not val:
                continue
            cond = cond + f" AND {col} = {self.obj_to_str(val)}"
        for col, val in cond_neq.items():
            if not val:
                continue
            cond = cond + f" AND {col} <> {self.obj_to_str(val)}"
        for col, val in cond_leq.items():
            if not val:
                continue
            cond = cond + f" AND {col} <= {self.obj_to_str(val)}"
        for col, val in cond_geq.items():
            if not val:
                continue
            cond = cond + f" AND {col} >= {self.obj_to_str(val)}"
        for col, val in cond_l.items():
            if not val:
                continue
            cond = cond + f" AND {col} < {self.obj_to_str(val)}"
        for col, val in cond_g.items():
            if not val:
                continue
            cond = cond + f" AND {col} > {self.obj_to_str(val)}"
        return cond

    def delete(
        self,
        table_name: str,
        cond_null: list[str] = list(),
        cond_not_null: list[str] = list(),
        cond_in: dict[str, list] = dict(),
        cond_eq: dict[str, object] = dict(),
        cond_neq: dict[str, object] = dict(),
        cond_leq: dict[str, object] = dict(),
        cond_geq: dict[str, object] = dict(),
        cond_l: dict[str, object] = dict(),
        cond_g: dict[str, object] = dict(),
        silent: bool = False,
    ) -> tuple[dict[str, object], ...]:
        """Delete rows from a database table based on conditions.

        Parameters
        ----------
        table_name : str
            Name of the table to delete from
        cond_null : list[str], optional
            Columns that must be NULL
        cond_not_null : list[str], optional
            Columns that must not be NULL
        cond_in : dict[str, list], optional
            Column values that must be in given list
        cond_eq : dict[str, object], optional
            Column values that must equal given value
        cond_neq : dict[str, object], optional
            Column values that must not equal given value
        cond_leq : dict[str, object], optional
            Column values that must be less than or equal to given value
        cond_geq : dict[str, object], optional
            Column values that must be greater than or equal to given value
        cond_l : dict[str, object], optional
            Column values that must be less than given value
        cond_g : dict[str, object], optional
            Column values that must be greater than given value
        silent : bool, optional
            If True, suppress logging of the query execution, by default False

        Returns
        -------
        tuple
            Tuple containing the deleted rows' data

        Raises
        ------
        NoConnectionError
            If no database connection exists
        MySqlWrongQueryError
            If query is wrong
        """
        res_mysql = self.select(
            table_name=table_name,
            cond_eq=cond_eq,
            cond_g=cond_g,
            cond_geq=cond_geq,
            cond_in=cond_in,
            cond_l=cond_l,
            cond_leq=cond_leq,
            cond_neq=cond_neq,
            cond_not_null=cond_not_null,
            cond_null=cond_null,
            silent=True,
        )
        query = f"DELETE FROM {table_name} "
        query = query + self.generate_cond(
            cond_eq=cond_eq,
            cond_g=cond_g,
            cond_geq=cond_geq,
            cond_in=cond_in,
            cond_l=cond_l,
            cond_leq=cond_leq,
            cond_neq=cond_neq,
            cond_not_null=cond_not_null,
            cond_null=cond_null,
        )
        query = query + ";"
        try:
            self.execute(query=query, silent=silent)
        except MySqlWrongQueryError as e:
            self.logger.warning(
                f"wrong query when trying to update by id, {type(e)=}, {str(e)}, {traceback.print_exc()}"
            )
            raise e
        self.connection.commit()  # type: ignore
        return res_mysql

    def execute(
        self, query: str, args: tuple | dict | None = None, silent=False
    ) -> tuple[dict[str, object], ...]:
        """Execute a SQL query and return the results.

        Parameters
        ----------
        query : str
            SQL query to execute
        args : tuple | dict | None, optional
            Parameters to pass to the query, by default None
        silent : bool, optional
            If True, suppress logging of the query execution, by default False

        Returns
        -------
        tuple
            Results of the query execution

        Raises
        ------
        NoConnectionError
            If no database connection exists
        MySqlWrongQueryError
            If query is wrong
        """
        if not self.connection:
            self.logger.error("could not execute query, no connection to Database")
            raise MySqlNoConnectionError()
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(query=query, args=args)
                res = cursor.fetchall()
            except pymysql.err.ProgrammingError as e:
                self.logger.warning(
                    f"error while executing query {type(e)=}, {str(e)}, {traceback.print_exc()}"
                )
                raise MySqlWrongQueryError(f"{type(e)=}, {str(e)=}")
            if not silent:
                self.logging(cursor)
        return res

    def count(
        self,
        table_name: str,
        select_col: list[str] = list(),
        cond_null: list[str] = list(),
        cond_not_null: list[str] = list(),
        cond_in: dict[str, list] = dict(),
        cond_eq: dict[str, object] = dict(),
        cond_neq: dict[str, object] = dict(),
        cond_leq: dict[str, object] = dict(),
        cond_geq: dict[str, object] = dict(),
        cond_l: dict[str, object] = dict(),
        cond_g: dict[str, object] = dict(),
        silent: bool = False,
    ) -> int | None:
        """Execute a SELECT COUNT(...) query with various conditions.

        Parameters
        ----------
        table_name : str
            Name of the table to query
        select_col : list[str], optional
            List of columns to include in the COUNT(...), by default all columns
        cond_null : list[str], optional
            Columns that must be NULL
        cond_not_null : list[str], optional
            Columns that must not be NULL
        cond_in : dict[str, list], optional
            Column values that must be in given list
        cond_eq : dict[str, object], optional
            Column values that must equal given value
        cond_neq : dict[str, object], optional
            Column values that must not equal given value
        cond_leq : dict[str, object], optional
            Column values that must be less than or equal to given value
        cond_geq : dict[str, object], optional
            Column values that must be greater than or equal to given value
        cond_l : dict[str, object], optional
            Column values that must be less than given value
        cond_g : dict[str, object], optional
            Column values that must be greater than given value
        silent : bool, optional
            If True, suppress logging of the query execution, by default False

        Returns
        -------
        int
            result of the count
        None
            if query went wrong

        Raises
        ------
        NoConnectionError
            If no database connection exists
        MySqlWrongQueryError
            If query is wrong
        """
        query = f"SELECT COUNT({', '.join(select_col) if select_col else '*'}) AS ct FROM {table_name} "
        query = query + self.generate_cond(
            cond_eq=cond_eq,
            cond_g=cond_g,
            cond_geq=cond_geq,
            cond_in=cond_in,
            cond_l=cond_l,
            cond_leq=cond_leq,
            cond_neq=cond_neq,
            cond_not_null=cond_not_null,
            cond_null=cond_null,
        )
        query = query + ";"

        res_mysql = self.execute(query=query, silent=silent)
        if not res_mysql:
            return None
        res = res_mysql[0].get("ct", None)
        return int(str(res)) if res else None

    def select(
        self,
        table_name: str,
        select_col: list[str] = list(),
        cond_null: list[str] = list(),
        cond_not_null: list[str] = list(),
        cond_in: dict[str, list] = dict(),
        cond_eq: dict[str, object] = dict(),
        cond_neq: dict[str, object] = dict(),
        cond_leq: dict[str, object] = dict(),
        cond_geq: dict[str, object] = dict(),
        cond_l: dict[str, object] = dict(),
        cond_g: dict[str, object] = dict(),
        order_by: str = "",
        ascending_order: bool = True,
        limit: int = 0,
        offset: int = 0,
        silent: bool = False,
    ) -> tuple[dict[str, object], ...]:
        """Execute a SELECT query with various conditions.

        Parameters
        ----------
        table_name : str
            Name of the table to query
        select_col : list[str], optional
            List of columns to select, by default all columns
        cond_null : list[str], optional
            Columns that must be NULL
        cond_not_null : list[str], optional
            Columns that must not be NULL
        cond_in : dict[str, list], optional
            Column values that must be in given list
        cond_eq : dict[str, object], optional
            Column values that must equal given value
        cond_neq : dict[str, object], optional
            Column values that must not equal given value
        cond_leq : dict[str, object], optional
            Column values that must be less than or equal to given value
        cond_geq : dict[str, object], optional
            Column values that must be greater than or equal to given value
        cond_l : dict[str, object], optional
            Column values that must be less than given value
        cond_g : dict[str, object], optional
            Column values that must be greater than given value
        silent : bool, optional
            If True, suppress logging of the query execution, by default False
        limit : int | None, optional
            Maximum number of rows to return, 0 means alls, by default 0
        offset : int | None, optional
            Number of rows to skip before returning results, 0 means no offset, by default 0

        Returns
        -------
        tuple
            Query results as a tuple of dictionaries

        Raises
        ------
        NoConnectionError
            If no database connection exists
        MySqlWrongQueryError
            If query is wrong
        """
        query = (
            f"SELECT {', '.join(select_col) if select_col else '*'} FROM {table_name} "
        )
        query = query + self.generate_cond(
            cond_eq=cond_eq,
            cond_g=cond_g,
            cond_geq=cond_geq,
            cond_in=cond_in,
            cond_l=cond_l,
            cond_leq=cond_leq,
            cond_neq=cond_neq,
            cond_not_null=cond_not_null,
            cond_null=cond_null,
        )
        if order_by:
            query = (
                query + f" ORDER BY {order_by} {'ASC' if ascending_order else 'DESC'} "
            )
        if limit:
            query = query + f" LIMIT {limit} "
            query = query + f" OFFSET {offset} "
        query = query + ";"

        res_mysql = self.execute(query=query, silent=silent)
        return res_mysql

    def select_by_id(
        self,
        table_name: str,
        id: str,
        select_col: list[str] = list(),
        silent: bool = False,
    ) -> dict:
        """Select a row from a database table by its ID.

        Parameters
        ----------
        table_name : str
            Name of the table to select from
        id : str
            ID of the row to select
        select_col : list[str], optional
            List of columns to select, by default all columns
        silent : bool, optional
            If True, suppress logging of the query execution, by default False

        Returns
        -------
        dict
            Dictionary containing the row's data, empty dict if not found

        Raises
        ------
        NoConnectionError
            If no database connection exists
        MySqlWrongQueryError
            If query is wrong
        """
        res_mysql = self.select(
            table_name=table_name,
            select_col=select_col,
            cond_eq={"id": id},
            silent=silent,
        )
        if not res_mysql:
            return dict()
        return res_mysql[0]

    def delete_by_id(self, table_name: str, id: str, silent: bool = False) -> dict:
        """Delete a row from a database table by its ID.

        Parameters
        ----------
        table_name : str
            Name of the table to delete from
        id : str
            ID of the row to delete
        silent : bool, optional
            If True, suppress logging of the query execution, by default False

        Returns
        -------
        dict
            Dictionary containing the deleted row's data, empty dict if not found

        Raises
        ------
        NoConnectionError
            If no database connection exists
        MySqlWrongQueryError
            If query is wrong
        """
        try:
            res_mysql = self.delete(
                table_name=table_name, cond_eq={"id": id}, silent=silent
            )
        except MySqlWrongQueryError as e:
            self.logger.warning(
                f"wrong query when trying to delete by id, {type(e)=}, {str(e)}, {traceback.print_exc()}"
            )
            raise e
        self.connection.commit()  # type: ignore
        return res_mysql[0] if res_mysql else dict()

    def close(self):
        if self.connection:
            self.connection.close()

    def insert_one(self, table_name: str, values: dict[str, object], silent=False):
        """Insert a single row into a database table.

        Parameters
        ----------
        table_name : str
            Name of the table to insert into
        values : dict
            Dictionary of column names and their corresponding values
        silent : bool, optional
            If True, suppress logging of the query execution, by default False

        Raises
        ------
        NoValueInsertionError
            If values dictionary is empty
        NoConnectionError
            If no database connection exists
        MySqlWrongQueryError
            If query is wrong
        """
        if not values:
            self.logger.warning("could not insert one, no values given")
            raise MySqlNoValueInsertionError()
        query = f"""
        INSERT INTO {table_name}
        ({", ".join([v for v in values])})
        VALUES ({", ".join(["%s"]*len(values))})
        """
        try:
            self.execute(
                query=query, args=tuple(v for v in values.values()), silent=silent
            )
        except MySqlWrongQueryError as e:
            self.logger.warning(
                f"wrong query when trying to insert one, {type(e)=}, {str(e)}, {traceback.print_exc()}"
            )
            raise e
        self.connection.commit()  # type: ignore

    def update(
        self,
        table_name: str,
        update_col_col: dict[str, str] = dict(),
        update_col_value: dict[str, object] = dict(),
        cond_null: list[str] = list(),
        cond_not_null: list[str] = list(),
        cond_in: dict[str, list] = dict(),
        cond_eq: dict[str, object] = dict(),
        cond_neq: dict[str, object] = dict(),
        cond_leq: dict[str, object] = dict(),
        cond_geq: dict[str, object] = dict(),
        cond_l: dict[str, object] = dict(),
        cond_g: dict[str, object] = dict(),
        silent: bool = False,
    ) -> tuple[dict[str, object], ...]:
        """Update rows in a database table based on conditions.

        Parameters
        ----------
        table_name : str
            Name of the table to update
        update_col_col : dict[str, str], optional
            Dictionary mapping columns to update with other column values
        update_col_value : dict[str, object], optional
            Dictionary mapping columns to update with specific values
        cond_null : list[str], optional
            Columns that must be NULL
        cond_not_null : list[str], optional
            Columns that must not be NULL
        cond_in : dict[str, list], optional
            Column values that must be in given list
        cond_eq : dict[str, object], optional
            Column values that must equal given value
        cond_neq : dict[str, object], optional
            Column values that must not equal given value
        cond_leq : dict[str, object], optional
            Column values that must be less than or equal to given value
        cond_geq : dict[str, object], optional
            Column values that must be greater than or equal to given value
        cond_l : dict[str, object], optional
            Column values that must be less than given value
        cond_g : dict[str, object], optional
            Column values that must be greater than given value
        silent : bool, optional
            If True, suppress logging of the query execution, by default False

        Returns
        -------
        tuple
            Updated rows' data

        Raises
        ------
        NoConnectionError
            If no database connection exists
        DuplicateColumnUpdateError
            If a column appears in both update_col_col and update_col_value
        MySqlWrongQueryError
            If query is wrong
        """
        if not update_col_col and not update_col_value:
            raise MySqlNoUpdateValuesError()

        for col in update_col_col:
            if col in update_col_value:
                raise (MySqlDuplicateColumnUpdateError(column=col))
        for col in update_col_value:
            if col in update_col_col:
                raise (MySqlDuplicateColumnUpdateError(column=col))

        try:
            ids_to_update = self.select(
                table_name=table_name,
                select_col=["id"],
                cond_eq=cond_eq,
                cond_g=cond_g,
                cond_geq=cond_geq,
                cond_in=cond_in,
                cond_l=cond_l,
                cond_leq=cond_leq,
                cond_neq=cond_neq,
                cond_not_null=cond_not_null,
                cond_null=cond_null,
                silent=True,
            )
        except Exception as e:
            self.logger.warning(
                f"error when trying to get the ids to update, {type(e)=}, {str(e)}, {traceback.print_exc()}"
            )
            raise e
        ids_to_update_ls = [str(dt["id"]) for dt in ids_to_update]
        if not ids_to_update:
            self.logger.info("nothing to update")
            return tuple()

        for col in update_col_value:
            update_col_value[col] = self.obj_to_str(update_col_value[col])

        query = f"UPDATE {table_name} SET "
        update_col = update_col_col | update_col_value
        update_ls = [f" {col} = {update_col[col]} " for col in update_col]
        query = query + f" {', '.join(update_ls)} "
        query = query + f""" WHERE id IN ('{"', '".join(ids_to_update_ls)}')"""
        try:
            self.execute(query=query, silent=silent)
        except MySqlWrongQueryError as e:
            self.logger.warning(
                f"wrong query when trying to update, {type(e)=}, {str(e)}, {traceback.print_exc()}"
            )
            raise e
        self.connection.commit()  # type: ignore

        return self.select(table_name=table_name, cond_in={"id": ids_to_update_ls})

    def update_by_id(
        self, table_name: str, id: str, values: dict[str, object], silent=False
    ) -> dict:
        """Update a single row in a table by its ID.

        Parameters
        ----------
        table_name : str
            Name of the table to update
        id : str
            ID of the row to update
        values : dict[str, object]
            Dictionary of column names and their new values
        silent : bool, optional
            If True, suppress logging of the query execution, by default False

        Returns
        -------
        dict
            Updated row data, empty dict if row not found

        Raises
        ------
        NoConnectionError
            If no database connection exists
        DuplicateColumnUpdateError
            If a column appears in both update_col_col and update_col_value
        MySqlWrongQueryError
            If query is wrong
        """
        try:
            mysql_res = self.update(
                table_name=table_name,
                update_col_value={k: v for (k, v) in values.items() if k != "id"},
                cond_eq={"id": id},
                silent=silent,
            )
        except MySqlWrongQueryError as e:
            self.logger.warning(
                f"wrong query when trying to update by id, {type(e)=}, {str(e)}, {traceback.print_exc()}"
            )
            raise e
        return mysql_res[0] if mysql_res else dict()

    def id_exists(self, table_name: str, id: str, silent: bool = False) -> bool:
        res = self.select_by_id(table_name=table_name, id=id, silent=silent)
        if res:
            return True
        return False
